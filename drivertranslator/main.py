from __future__ import annotations

import argparse
import asyncio
import contextlib
import html
import json
import logging
import secrets
import socket
import subprocess
import time
import urllib.parse
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from .models import (
    Config,
    ControllerState,
    HealthState,
    NhdCtlSession,
    ProblemState,
    RuntimeSettings,
    Rx,
    Tx,
)
from .protocol_helpers import (
    all_endpoint_aliases as _all_endpoint_aliases,
    as_success as _as_success,
    device_status_rx_dict as _device_status_rx_dict,
    device_status_tx_dict as _device_status_tx_dict,
    format_matrix_info as _format_matrix_info,
    format_tx_signal as _format_tx_signal,
    lookup_rx as _lookup_rx,
    lookup_tx as _lookup_tx,
    tx_alias_from_amx_stream as _tx_alias_from_amx_stream,
)
from .utils import (
    as_bool as _as_bool,
    as_int as _as_int,
    bind_addr as _bind_addr,
    clamp_int as _clamp_int,
    opt_str as _opt_str,
    retry_delay_seconds as _retry_delay_seconds,
    rx_alias_sort_key as _rx_alias_sort_key,
    tx_alias_sort_key as _tx_alias_sort_key,
)
from .config_loader import load_config, validate_config as _validate_config
from .unknown_ctl import (
    clear_persisted as _unknown_ctl_clear_persisted,
    configure as _unknown_ctl_configure,
    load_from_disk as _unknown_ctl_load_from_disk,
    page_text as _unknown_ctl_page_text,
    persist_file as _unknown_ctl_persist_file,
    record as _unknown_ctl_record,
)
from .networking import crlf_line as _crlf, open_connection as _open_connection
from .amx_protocol import (
    hdmi_enabled_from_status_fields as _hdmi_enabled_from_status_fields,
    log_amx_inbound as _log_amx_inbound,
    parse_amx_status as _parse_amx_status,
)
from .amx_client import AmxClient, DryRunAmxClient, PersistentAmxClient
from .http_ui_session import (
    issue_session_token as _http_ui_sess_issue,
    parse_path_params as _http_parse_path_params,
    valid_session_token as _http_ui_sess_valid,
)
from .log_ring import RingBufferLogHandler as _RingBufferLogHandler, get_log_tail as _get_log_tail
from .problem_reporter import LocalProblemReporter
from .system_control import do_reboot as _do_reboot, do_service_restart as _do_service_restart
from .config_persistence import (
    ctl_json as _ctl_json,
    generate_endpoints_from_size as _generate_endpoints_from_size,
    load_endpoint_inventory as _load_endpoint_inventory,
    persist_endpoint_skip_to_config as _persist_endpoint_skip_to_config,
    persist_endpoints_to_config as _persist_endpoints_to_config,
    persist_runtime_setting_to_config as _persist_runtime_setting_to_config,
)
from .http_helpers import (
    build_status_snapshot as _build_status_snapshot,
    control_feedback_html as _control_feedback_html,
    format_uptime as _format_uptime,
    http_response as _http_response,
    http_unauthorized as _http_unauthorized,
    params_want_html as _params_want_html,
    parse_basic_auth_password as _parse_basic_auth_password,
)

# Quick index (major sections in this file):
# - Unknown-command tracking: unknown_ctl.py
# - Config loading/validation: config_loader.py; JSON edits: config_persistence.py
# - HTTP response/snapshot helpers: http_helpers.py
# - TCP/AMX: networking.py, amx_protocol.py, amx_client.py
# - Log ring: log_ring.py; HTTP UI session tokens: http_ui_session.py
# - LocalProblemReporter: problem_reporter.py; reboot/restart: system_control.py
# - HTTP status + control API/UI
# - Shared controller/runtime state (models.py)
# - RTI/NHD-CTL protocol helpers and command handlers
# - Server bootstrap + process entrypoint
LOG = logging.getLogger("drivertranslator")

_TX_STATUS_POLL_INTERVAL_SECONDS = 30


async def _amx_self_test(*, cfg: Config, amx: Any) -> Dict[str, Any]:
    """
    Connectivity self-test: attempt to connect to each configured decoder IP.
    Returns a summary suitable for web UI and/or problem notification.
    """
    active_rx = [rx for rx in cfg.rx_by_alias.values() if rx.alias not in cfg.rx_skipped_aliases]
    if cfg.amx_dry_run:
        return {"ok": len(active_rx), "total": len(active_rx), "unreachable": []}

    ok = 0
    unreachable: List[str] = []
    local_addr = (cfg.amx_bind_address, 0) if cfg.amx_bind_address else None

    for rx in active_rx:
        ip = rx.amx_decoder_ip
        try:
            _r, w = await _open_connection(
                ip,
                cfg.amx_decoder_port,
                timeout=cfg.amx_connect_timeout_ms / 1000,
                local_addr=local_addr,
            )
            w.close()
            with contextlib.suppress(Exception):
                await w.wait_closed()
            ok += 1
        except Exception:
            unreachable.append(ip)

    return {"ok": ok, "total": len(active_rx), "unreachable": unreachable}


# ---------------------------------------------------------------------------
# HTTP status and control surface
# ---------------------------------------------------------------------------
async def _handle_http_client(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    *,
    cfg: Config,
    health: HealthState,
    amx: Any,
    state: ControllerState,
    runtime: RuntimeSettings,
    problems: ProblemState,
    started_at: float,
    config_path: str,
) -> None:
    try:
        try:
            data = await asyncio.wait_for(reader.read(4096), timeout=2.0)
        except asyncio.TimeoutError:
            # Common with port scans / health checks that connect but don't send a request.
            return
        if not data:
            return

        line = data.split(b"\r\n", 1)[0].decode("ascii", errors="replace")
        parts = line.split()
        if len(parts) < 2:
            writer.write(_http_response("400 Bad Request", "text/plain", b"bad request"))
            return
        method, path = parts[0], parts[1]
        path_only, early_params = _http_parse_path_params(path)
        control_via_ui = path_only.startswith("/control/") and _http_ui_sess_valid(
            early_params.get("ui_sess", "")
        )

        # Require Basic auth (except /control/* with valid ui_sess from this session's status page).
        if cfg.http_status_password:
            pw = _parse_basic_auth_password(data)
            if pw != cfg.http_status_password and not control_via_ui:
                writer.write(_http_unauthorized())
                return

        if method != "GET":
            writer.write(_http_response("405 Method Not Allowed", "text/plain", b"method not allowed"))
            return

        snapshot = _build_status_snapshot(cfg=cfg, health=health, amx=amx, started_at=started_at)
        rt = await runtime.snapshot()
        _ = await problems.snapshot()

        if path in ("/status", "/status.json"):
            body = (json.dumps(snapshot, indent=2) + "\n").encode("utf-8")
            writer.write(_http_response("200 OK", "application/json", body))
            return

        if path in ("/logs", "/logs.json"):
            body = (json.dumps({"lines": _get_log_tail(rt["http_log_lines"])}, indent=2) + "\n").encode(
                "utf-8"
            )
            writer.write(_http_response("200 OK", "application/json", body))
            return

        if path in ("/control", "/control.json"):
            body = (json.dumps(rt, indent=2) + "\n").encode("utf-8")
            writer.write(_http_response("200 OK", "application/json", body))
            return

        if path in ("/problems", "/problems.json"):
            prob = await problems.snapshot()
            body = (json.dumps({"problems": prob}, indent=2) + "\n").encode("utf-8")
            writer.write(_http_response("200 OK", "application/json", body))
            return

        # Basic control endpoints (optional token).
        # /control/set?key=<k>&value=<v>[&token=<t>]
        # /control/set_endpoints?tx_count=<n>&rx_count=<n>&tx_start_ip=<ip>&rx_start_ip=<ip>
        # /control/set_endpoint_skip?kind=tx|rx&alias=<alias>&skip=true|false
        # /control/selftest?[token=<t>]
        # /control/restart
        # /control/reboot
        # /control/clear_unknown_ctl
        if path.startswith("/control/"):
            # very small query parsing (no urllib dependency)
            qs = ""
            if "?" in path:
                qs = path.split("?", 1)[1]
            params: Dict[str, str] = {}
            for part in qs.split("&"):
                if "=" in part:
                    k, v = part.split("=", 1)
                    params[k] = v

            token = cfg.http_status_control_token
            if token and params.get("token") != token:
                if _params_want_html(params):
                    writer.write(
                        _http_response(
                            "403 Forbidden",
                            "text/html; charset=utf-8",
                            _control_feedback_html(
                                ok=False,
                                headline="Not allowed",
                                paragraphs=[
                                    "Wrong or missing control token. "
                                    "Set it in config (http_status.control_token) and use the same value in the link."
                                ],
                            ),
                        )
                    )
                else:
                    writer.write(_http_response("403 Forbidden", "text/plain", b"forbidden"))
                return

            ctl_via = "status_page" if params.get("ui_sess") else "http_api"

            if path.startswith("/control/set_endpoints"):
                want_html = _params_want_html(params)
                tx_count_s = params.get("tx_count", "").strip()
                rx_count_s = params.get("rx_count", "").strip()
                tx_start_ip = params.get("tx_start_ip", "").strip()
                rx_start_ip = params.get("rx_start_ip", "").strip()
                try:
                    if not tx_count_s or not rx_count_s or not tx_start_ip or not rx_start_ip:
                        raise ValueError("All parameters are required.")
                    res = _persist_endpoints_to_config(
                        config_path=config_path,
                        tx_count=int(tx_count_s),
                        rx_count=int(rx_count_s),
                        tx_start_ip=tx_start_ip,
                        rx_start_ip=rx_start_ip,
                    )
                except Exception as e:
                    LOG.warning(
                        "HTTP control [source=%s]: set_endpoints rejected tx_count=%r rx_count=%r tx_start_ip=%r rx_start_ip=%r err=%s",
                        ctl_via,
                        tx_count_s,
                        rx_count_s,
                        tx_start_ip,
                        rx_start_ip,
                        e,
                    )
                    bad_msg = (
                        "Expected tx_count/rx_count as numbers (1-512) and tx_start_ip/rx_start_ip as valid IPv4 addresses."
                    )
                    if want_html:
                        writer.write(
                            _http_response(
                                "400 Bad Request",
                                "text/html; charset=utf-8",
                                _control_feedback_html(
                                    ok=False,
                                    headline="Could not apply endpoint sizing",
                                    paragraphs=[bad_msg],
                                ),
                            )
                        )
                    else:
                        body = (
                            json.dumps({"ok": False, "error": bad_msg}, indent=2) + "\n"
                        ).encode("utf-8")
                        writer.write(_http_response("400 Bad Request", "application/json", body))
                    return
                if want_html:
                    writer.write(
                        _http_response(
                            "200 OK",
                            "text/html; charset=utf-8",
                            _control_feedback_html(
                                ok=True,
                                headline="Endpoint sizing saved",
                                paragraphs=[
                                    f"Configured TX={res['tx_count']} starting at {res['tx_start_ip']}; RX={res['rx_count']} starting at {res['rx_start_ip']}.",
                                    "Saved to config. Restart DriverTranslator to apply new endpoint lists.",
                                ],
                                pre_json=res,
                            ),
                        )
                    )
                else:
                    body = (json.dumps(res, indent=2) + "\n").encode("utf-8")
                    writer.write(_http_response("200 OK", "application/json", body))
                LOG.info(
                    "HTTP control [source=%s]: set_endpoints tx_count=%s rx_count=%s tx_start_ip=%s rx_start_ip=%s (restart required)",
                    ctl_via,
                    res["tx_count"],
                    res["rx_count"],
                    res["tx_start_ip"],
                    res["rx_start_ip"],
                )
                return

            if path.startswith("/control/set_endpoint_skip"):
                want_html = _params_want_html(params)
                kind = params.get("kind", "").strip().lower()
                alias = params.get("alias", "").strip()
                skip_s = params.get("skip", "").strip().lower()
                try:
                    if skip_s in ("1", "true", "yes", "y", "on"):
                        skip = True
                    elif skip_s in ("0", "false", "no", "n", "off"):
                        skip = False
                    else:
                        raise ValueError("skip must be true or false")
                    res = _persist_endpoint_skip_to_config(
                        config_path=config_path,
                        kind=kind,
                        alias=alias,
                        skip=skip,
                    )
                except Exception as e:
                    LOG.warning(
                        "HTTP control [source=%s]: set_endpoint_skip rejected kind=%r alias=%r skip=%r err=%s",
                        ctl_via,
                        kind,
                        alias,
                        skip_s,
                        e,
                    )
                    bad_msg = "Expected kind=tx|rx, alias=<existing endpoint alias>, and skip=true|false."
                    if want_html:
                        writer.write(
                            _http_response(
                                "400 Bad Request",
                                "text/html; charset=utf-8",
                                _control_feedback_html(
                                    ok=False,
                                    headline="Could not update endpoint skip",
                                    paragraphs=[bad_msg],
                                ),
                            )
                        )
                    else:
                        body = (
                            json.dumps({"ok": False, "error": bad_msg}, indent=2) + "\n"
                        ).encode("utf-8")
                        writer.write(_http_response("400 Bad Request", "application/json", body))
                    return
                if want_html:
                    writer.write(
                        _http_response(
                            "200 OK",
                            "text/html; charset=utf-8",
                            _control_feedback_html(
                                ok=True,
                                headline="Endpoint skip updated",
                                paragraphs=[
                                    f"{res['kind'].upper()} {res['alias']} skip is now {str(res['skip']).lower()}.",
                                    "Saved to config. Restart DriverTranslator to apply.",
                                ],
                                pre_json=res,
                            ),
                        )
                    )
                else:
                    body = (json.dumps(res, indent=2) + "\n").encode("utf-8")
                    writer.write(_http_response("200 OK", "application/json", body))
                LOG.info(
                    "HTTP control [source=%s]: set_endpoint_skip %s %s skip=%s (restart required)",
                    ctl_via,
                    res["kind"],
                    res["alias"],
                    res["skip"],
                )
                return

            if path.startswith("/control/set"):
                key = params.get("key", "")
                value = params.get("value", "")
                want_html = _params_want_html(params)
                try:
                    if value.lower() in ("true", "1", "yes", "y", "on", "false", "0", "no", "n", "off"):
                        await runtime.set_bool(key, value.lower() in ("true", "1", "yes", "y", "on"))
                    else:
                        await runtime.set_int(key, int(value))
                    snap = await runtime.snapshot()
                    _persist_runtime_setting_to_config(
                        config_path=config_path,
                        key=key,
                        value=snap.get(key),
                    )
                    if key == "expanded_log" and hasattr(amx, "set_expanded_log"):
                        try:
                            amx.set_expanded_log(bool(snap.get("expanded_log")))
                        except Exception:
                            LOG.exception("Failed applying expanded_log to AMX client")
                except Exception:
                    LOG.warning(
                        "HTTP control [source=%s]: set rejected key=%r value=%r",
                        ctl_via,
                        key,
                        value,
                    )
                    bad_msg = (
                        "Expected key amx_dry_run, amx_persistent, amx_verify_after_set, or expanded_log with true/false, "
                        "or amx_verify_timeout_ms with a number (100-5000)."
                    )
                    if want_html:
                        writer.write(
                            _http_response(
                                "400 Bad Request",
                                "text/html; charset=utf-8",
                                _control_feedback_html(
                                    ok=False,
                                    headline="Could not apply setting",
                                    paragraphs=[bad_msg],
                                ),
                            )
                        )
                    else:
                        writer.write(_http_response("400 Bad Request", "text/plain", b"bad control request"))
                    return
                snap = await runtime.snapshot()
                if want_html:
                    if key == "amx_verify_timeout_ms":
                        paras = [
                            f"amx_verify_timeout_ms is now {snap.get('amx_verify_timeout_ms')} ms.",
                            "Takes effect immediately; no service restart needed.",
                        ]
                    elif key == "amx_dry_run":
                        v = snap.get("amx_dry_run")
                        paras = [
                            f"amx_dry_run is now {str(v).lower()}.",
                            "Saved to config. Restart DriverTranslator to apply this mode change.",
                        ]
                    elif key == "amx_persistent":
                        v = snap.get("amx_persistent")
                        paras = [
                            f"amx_persistent is now {str(v).lower()} ({'persistent' if v else 'non-persistent connect-close'} mode).",
                            "Saved to config. Restart DriverTranslator to apply this mode change.",
                        ]
                    elif key == "amx_verify_after_set":
                        v = snap.get("amx_verify_after_set")
                        paras = [
                            f"amx_verify_after_set is now {str(v).lower()} (post-route AMX STREAM check).",
                            "Takes effect immediately; no service restart needed.",
                        ]
                    elif key == "expanded_log":
                        v = snap.get("expanded_log")
                        paras = [
                            f"expanded_log is now {str(v).lower()} (logs RTI responses and AMX replies).",
                            "Takes effect immediately; no service restart needed.",
                        ]
                    else:
                        paras = [
                            f"Updated setting {key!r}.",
                            "Takes effect immediately; no service restart needed.",
                        ]
                    writer.write(
                        _http_response(
                            "200 OK",
                            "text/html; charset=utf-8",
                            _control_feedback_html(
                                ok=True,
                                headline="Saved",
                                paragraphs=paras,
                                pre_json={
                                    "amx_dry_run": snap.get("amx_dry_run"),
                                    "amx_persistent": snap.get("amx_persistent"),
                                    "amx_verify_after_set": snap.get("amx_verify_after_set"),
                                    "amx_verify_timeout_ms": snap.get("amx_verify_timeout_ms"),
                                    "expanded_log": snap.get("expanded_log"),
                                },
                            ),
                        )
                    )
                else:
                    body = (json.dumps(snap, indent=2) + "\n").encode("utf-8")
                    writer.write(_http_response("200 OK", "application/json", body))
                LOG.info(
                    "HTTP control [source=%s]: set %s=%r (dry_run=%s persistent=%s verify_after_set=%s verify_timeout_ms=%s expanded_log=%s)",
                    ctl_via,
                    key,
                    snap.get(key),
                    snap.get("amx_dry_run"),
                    snap.get("amx_persistent"),
                    snap.get("amx_verify_after_set"),
                    snap.get("amx_verify_timeout_ms"),
                    snap.get("expanded_log"),
                )
                return

            if path.startswith("/control/selftest"):
                res = await _amx_self_test(cfg=cfg, amx=amx)
                total = int(res.get("total") or 0)
                ok_n = int(res.get("ok") or 0)
                all_ok = total == 0 or ok_n == total
                unr = res.get("unreachable") or []
                LOG.info(
                    "HTTP control [source=%s]: self-test %s/%s decoders reachable%s",
                    ctl_via,
                    ok_n,
                    total,
                    f" unreachable={unr!r}" if unr else "",
                )
                if _params_want_html(params):
                    if total == 0:
                        paras = ["No RX / decoders are configured in this profile."]
                    else:
                        paras = [f"TCP connect to port {cfg.amx_decoder_port}: {ok_n} of {total} reachable."]
                        if unr:
                            paras.append("Unreachable: " + ", ".join(str(x) for x in unr))
                        else:
                            paras.append("All configured decoder IPs accepted a connection.")
                    writer.write(
                        _http_response(
                            "200 OK",
                            "text/html; charset=utf-8",
                            _control_feedback_html(
                                ok=all_ok,
                                headline="Self-test passed" if all_ok else "Self-test: issues found",
                                paragraphs=paras,
                                pre_json=res,
                            ),
                        )
                    )
                else:
                    body = (json.dumps(res, indent=2) + "\n").encode("utf-8")
                    writer.write(_http_response("200 OK", "application/json", body))
                return

            if path.startswith("/control/clear_unknown_ctl"):
                _unknown_ctl_clear_persisted()
                LOG.info("HTTP control [source=%s]: cleared unknown_ctl list", ctl_via)
                if _params_want_html(params):
                    writer.write(
                        _http_response(
                            "200 OK",
                            "text/html; charset=utf-8",
                            _control_feedback_html(
                                ok=True,
                                headline="List cleared",
                                paragraphs=[
                                    "The unrecognized-command list is empty.",
                                    "Reload the status page to refresh the box below.",
                                ],
                            ),
                        )
                    )
                else:
                    body = (json.dumps({"ok": True, "cleared": True}, indent=2) + "\n").encode("utf-8")
                    writer.write(_http_response("200 OK", "application/json", body))
                return

            if path.startswith("/control/restart"):
                if _params_want_html(params):
                    writer.write(
                        _http_response(
                            "200 OK",
                            "text/html; charset=utf-8",
                            _control_feedback_html(
                                ok=True,
                                headline="Service restart scheduled",
                                paragraphs=[
                                    "DriverTranslator service will restart shortly.",
                                    "This page connection may briefly drop while the process restarts.",
                                ],
                            ),
                        )
                    )
                else:
                    body = (json.dumps({"ok": True, "action": "service_restart"}, indent=2) + "\n").encode("utf-8")
                    writer.write(_http_response("200 OK", "application/json", body))
                LOG.warning("HTTP control [source=%s]: service restart requested", ctl_via)
                asyncio.create_task(_do_service_restart(reason=f"http_control:{ctl_via}"))
                return

            if path.startswith("/control/reboot"):
                if _params_want_html(params):
                    writer.write(
                        _http_response(
                            "200 OK",
                            "text/html; charset=utf-8",
                            _control_feedback_html(
                                ok=True,
                                headline="Reboot scheduled",
                                paragraphs=[
                                    "The machine will restart shortly. This page and SSH will drop until the system is back.",
                                    "Open the status page again after boot if you need to confirm the service.",
                                ],
                            ),
                        )
                    )
                else:
                    body = (json.dumps({"ok": True, "action": "rebooting"}, indent=2) + "\n").encode("utf-8")
                    writer.write(_http_response("200 OK", "application/json", body))
                LOG.warning("HTTP control [source=%s]: host reboot requested", ctl_via)
                asyncio.create_task(_do_reboot(reason=f"http_control:{ctl_via}"))
                return

            writer.write(_http_response("404 Not Found", "text/plain", b"not found"))
            return

        if path == "/" or path.startswith("/?"):
            uptime_h = _format_uptime(int(snapshot["uptime_seconds"]))
            amx_conn = (
                f"{snapshot['amx_connected']}/{max(snapshot['amx_total_known'] or 0, snapshot['rx_configured'])}"
                if snapshot["amx_connected"] is not None
                else "n/a"
            )
            log_lines = "\n".join(_get_log_tail(rt["http_log_lines"]))
            tx_aliases = sorted(cfg.tx_by_alias.keys(), key=_tx_alias_sort_key)
            rx_aliases = sorted(cfg.rx_by_alias.keys(), key=_rx_alias_sort_key)
            tx_start_ip = cfg.tx_by_alias[tx_aliases[0]].ip if tx_aliases else ""
            rx_start_ip = cfg.rx_by_alias[rx_aliases[0]].ip if rx_aliases else ""
            endpoint_inventory = _load_endpoint_inventory(config_path=config_path)
            tx_skip_by_alias = {
                str(row.get("alias", "")): _as_bool(row.get("skip"), default=False)
                for row in endpoint_inventory.get("tx", [])
            }
            rx_skip_by_alias = {
                str(row.get("alias", "")): _as_bool(row.get("skip"), default=False)
                for row in endpoint_inventory.get("rx", [])
            }
            route_rows = []
            tx_all_aliases = sorted(
                set(tx_aliases).union({a for a in tx_skip_by_alias.keys() if a}),
                key=_tx_alias_sort_key,
            )
            rx_all_aliases = sorted(
                set(rx_aliases).union({a for a in rx_skip_by_alias.keys() if a}),
                key=_rx_alias_sort_key,
            )
            for tx_alias in tx_all_aliases:
                tx_is_skip = bool(tx_skip_by_alias.get(tx_alias, False))
                tx_next_skip = "false" if tx_is_skip else "true"
                tx_skip_btn = (
                    f"<button type=\"button\" class=\"ctrl-run\" data-dt-ctl=\"set_endpoint_skip\" "
                    f"data-kind=\"tx\" data-alias=\"{html.escape(tx_alias)}\" data-skip=\"{tx_next_skip}\">"
                    f"{'Unskip' if tx_is_skip else 'Skip'}</button>"
                )
                tx = cfg.tx_by_alias.get(tx_alias)
                if tx_is_skip:
                    route_rows.append(
                        f"<tr><td><code>{html.escape(tx_alias)}</code></td><td><code>-</code></td><td class=\"bad\"><b>SKIPPED</b></td><td><b>-</b></td><td>{tx_skip_btn}</td></tr>"
                    )
                    continue
                if tx is None:
                    continue
                tx_fields = state.tx_status_fields.get(tx_alias) or {}
                polled_stream = (tx_fields.get("STREAM") or "").strip()
                stream_txt = f"STREAM {polled_stream}" if polled_stream else f"STREAM {tx.amx_stream}"
                tx_online = state.tx_online.get(tx_alias, False)
                tx_status_txt = "ONLINE" if tx_online else "OFFLINE"
                tx_status_cls = "ok" if tx_online else "bad"
                signal_txt, signal_cls = _format_tx_signal(tx_fields)
                route_rows.append(
                    f"<tr><td><code>{tx_alias}</code></td><td><code>{stream_txt}</code></td><td class=\"{tx_status_cls}\"><b>{tx_status_txt}</b></td><td class=\"{signal_cls}\"><b>{html.escape(signal_txt)}</b></td><td>{tx_skip_btn}</td></tr>"
                )
            for rx_alias in rx_all_aliases:
                rx_is_skip = bool(rx_skip_by_alias.get(rx_alias, False))
                rx_next_skip = "false" if rx_is_skip else "true"
                rx_skip_btn = (
                    f"<button type=\"button\" class=\"ctrl-run\" data-dt-ctl=\"set_endpoint_skip\" "
                    f"data-kind=\"rx\" data-alias=\"{html.escape(rx_alias)}\" data-skip=\"{rx_next_skip}\">"
                    f"{'Unskip' if rx_is_skip else 'Skip'}</button>"
                )
                if rx_is_skip:
                    route_rows.append(
                        f"<tr><td><code>{html.escape(rx_alias)}</code></td><td><code>NULL</code></td><td class=\"bad\"><b>SKIPPED</b></td><td><b>-</b></td><td>{rx_skip_btn}</td></tr>"
                    )
                    continue
                if rx_alias not in cfg.rx_by_alias:
                    continue
                tx_alias = state.video.get(rx_alias) or "NULL"
                online = state.rx_online.get(rx_alias, True)
                status_txt = "ONLINE" if online else "OFFLINE"
                status_cls = "ok" if online else "bad"
                hdmi_enabled = state.rx_hdmi_output.get(rx_alias)
                if hdmi_enabled is True:
                    hdmi_txt = "ON"
                    hdmi_cls = "ok"
                elif hdmi_enabled is False:
                    hdmi_txt = "OFF"
                    hdmi_cls = "bad"
                else:
                    hdmi_txt = "UNKNOWN"
                    hdmi_cls = ""
                route_rows.append(
                    f"<tr><td><code>{rx_alias}</code></td><td><code>{tx_alias}</code></td><td class=\"{status_cls}\"><b>{status_txt}</b></td><td class=\"{hdmi_cls}\"><b>{hdmi_txt}</b></td><td>{rx_skip_btn}</td></tr>"
                )
            route_html = "\n".join(route_rows)
            _ct = cfg.http_status_control_token or ""
            ctl_qs = ("&token=" + urllib.parse.quote_plus(_ct)) if _ct else ""
            _ui_sess = _http_ui_sess_issue()
            _ui_sess_js = json.dumps(_ui_sess)
            _ctl_qs_js = json.dumps(ctl_qs)
            _amx_port = int(cfg.amx_decoder_port)
            _unknown_pre = html.escape(_unknown_ctl_page_text())
            _uc_path = _unknown_ctl_persist_file()
            if _uc_path is not None:
                _uc_persist_note = (
                    "Stored on disk at <code>"
                    + html.escape(str(_uc_path))
                    + "</code> (survives restart). Use the button below the list to clear."
                )
            else:
                _uc_persist_note = (
                    "<b>Not persisted</b> (<code>unknown_ctl.enabled</code> is false in config); "
                    "restart clears this list. Set <code>unknown_ctl.enabled</code> to true to save beside config."
                )
            body = f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>DriverTranslator Status</title>
  <style>
    :root {{
      color-scheme: light dark;
      --accent: #2563eb;
      --accent-soft: rgba(37, 99, 235, 0.12);
      --bg: #f4f6f9;
      --fg: #0f172a;
      --muted: #64748b;
      --card: #ffffff;
      --border: #e2e8f0;
      --row: #f1f5f9;
      --code-bg: #f1f5f9;
      --code-fg: #0f172a;
      --log-bg: #0f172a;
      --log-fg: #e2e8f0;
      --link: #2563eb;
      --shadow: 0 1px 3px rgba(15, 23, 42, 0.06), 0 4px 14px rgba(15, 23, 42, 0.04);
    }}

    @media (prefers-color-scheme: dark) {{
      :root {{
        --accent: #60a5fa;
        --accent-soft: rgba(96, 165, 250, 0.12);
        --bg: #0c0f14;
        --fg: #f1f5f9;
        --muted: #94a3b8;
        --card: #141a24;
        --border: #273449;
        --row: #1a2332;
        --code-bg: #1e293b;
        --code-fg: #e2e8f0;
        --log-bg: #0a0e14;
        --log-fg: #cbd5e1;
        --link: #93c5fd;
        --shadow: 0 2px 8px rgba(0, 0, 0, 0.35);
      }}
    }}

    [data-theme="light"] {{
      --accent: #2563eb;
      --accent-soft: rgba(37, 99, 235, 0.12);
      --bg: #f4f6f9;
      --fg: #0f172a;
      --muted: #64748b;
      --card: #ffffff;
      --border: #e2e8f0;
      --row: #f1f5f9;
      --code-bg: #f1f5f9;
      --code-fg: #0f172a;
      --log-bg: #0f172a;
      --log-fg: #e2e8f0;
      --link: #2563eb;
      --shadow: 0 1px 3px rgba(15, 23, 42, 0.06), 0 4px 14px rgba(15, 23, 42, 0.04);
    }}

    [data-theme="dark"] {{
      --accent: #60a5fa;
      --accent-soft: rgba(96, 165, 250, 0.12);
      --bg: #0c0f14;
      --fg: #f1f5f9;
      --muted: #94a3b8;
      --card: #141a24;
      --border: #273449;
      --row: #1a2332;
      --code-bg: #1e293b;
      --code-fg: #e2e8f0;
      --log-bg: #0a0e14;
      --log-fg: #cbd5e1;
      --link: #93c5fd;
      --shadow: 0 2px 8px rgba(0, 0, 0, 0.35);
    }}

    * {{ box-sizing: border-box; }}
    body {{
      font-family: "Segoe UI", system-ui, -apple-system, Roboto, sans-serif;
      margin: 0;
      min-height: 100vh;
      background: var(--bg);
      color: var(--fg);
      line-height: 1.5;
    }}
    .wrap {{
      max-width: 820px;
      margin: 0 auto;
      padding: 28px 20px 48px;
    }}
    a {{ color: var(--link); text-decoration: none; }}
    a:hover {{ text-decoration: underline; }}

    .topbar {{
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 16px;
      margin-bottom: 28px;
      flex-wrap: wrap;
    }}
    .brand {{
      display: flex;
      flex-direction: column;
      gap: 2px;
    }}
    .brand h1 {{
      margin: 0;
      font-size: 1.45rem;
      font-weight: 700;
      letter-spacing: -0.02em;
    }}
    .brand span {{ font-size: 0.8rem; color: var(--muted); }}

    .btn {{
      border: 1px solid var(--border);
      background: var(--card);
      color: var(--fg);
      padding: 8px 14px;
      border-radius: 10px;
      cursor: pointer;
      font-size: 13px;
      font-weight: 500;
      box-shadow: var(--shadow);
      transition: transform 0.12s ease, border-color 0.12s;
    }}
    .btn:hover {{ border-color: var(--accent); }}
    .btn-primary {{
      background: var(--accent);
      color: #fff;
      border-color: transparent;
    }}
    .btn-primary:hover {{ filter: brightness(1.06); }}

    .card {{
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 14px;
      padding: 18px 20px;
      margin-bottom: 20px;
      box-shadow: var(--shadow);
    }}
    .card .row {{
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 12px;
      padding: 10px 0;
      border-bottom: 1px solid var(--border);
    }}
    .card .row:last-child {{ border-bottom: 0; }}

    .section-title {{
      font-size: 0.75rem;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      color: var(--muted);
      margin: 28px 0 10px 0;
      border-left: 3px solid var(--accent);
      padding-left: 10px;
    }}
    .section-title:first-of-type {{ margin-top: 0; }}

    code {{
      background: var(--code-bg);
      color: var(--code-fg);
      padding: 3px 8px;
      border-radius: 6px;
      font-size: 0.88em;
    }}
    .ok {{ color: #16a34a; font-weight: 600; }}
    .bad {{ color: #dc2626; font-weight: 600; }}

    .links-bar {{
      display: flex;
      flex-wrap: wrap;
      gap: 10px 16px;
      font-size: 12px;
      color: var(--muted);
      margin-bottom: 8px;
    }}
    .links-bar a {{ font-weight: 500; }}

    pre {{
      white-space: pre-wrap;
      background: var(--log-bg);
      color: var(--log-fg);
      padding: 16px 18px;
      border-radius: 12px;
      overflow-x: auto;
      font-size: 12px;
      line-height: 1.45;
      border: 1px solid var(--border);
    }}

    .table-wrap {{
      border-radius: 12px;
      border: 1px solid var(--border);
      overflow: hidden;
      box-shadow: var(--shadow);
      margin-bottom: 8px;
    }}
    table {{ border-collapse: collapse; width: 100%; background: var(--card); }}
    thead th {{
      background: var(--accent-soft);
      color: var(--fg);
      font-size: 11px;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      font-weight: 600;
      padding: 12px 14px;
      text-align: left;
      border-bottom: 1px solid var(--border);
    }}
    tbody tr {{ transition: background 0.1s; }}
    tbody tr:nth-child(even) {{ background: var(--row); }}
    tbody tr:hover {{ background: var(--accent-soft); }}
    tbody td {{ padding: 10px 14px; border-bottom: 1px solid var(--border); font-size: 14px; }}
    tbody tr:last-child td {{ border-bottom: 0; }}

    .subtle {{ color: var(--muted); font-size: 13px; line-height: 1.5; margin: 0 0 14px 0; max-width: 720px; }}

    .unk-ctl-actions {{
      margin: 14px 0 8px 0;
      padding-top: 12px;
      border-top: 1px solid var(--border);
    }}
    .unk-ctl-actions .ctrl-run {{
      padding: 10px 18px;
      border-radius: 10px;
      background: var(--accent);
      color: #fff;
      font-weight: 600;
      font-size: 14px;
      border: none;
      cursor: pointer;
      box-shadow: var(--shadow);
    }}
    .unk-ctl-actions .ctrl-run:hover {{ filter: brightness(1.06); }}
    .unk-ctl-actions .ctrl-run:disabled {{ opacity: 0.55; cursor: not-allowed; }}

    .help-icon {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      width: 18px;
      height: 18px;
      margin-left: 8px;
      font-size: 11px;
      font-weight: 700;
      border: 1px solid var(--border);
      border-radius: 50%;
      color: var(--muted);
      background: var(--row);
      cursor: help;
      vertical-align: middle;
      line-height: 1;
    }}
    .help-icon:hover {{ color: var(--accent); border-color: var(--accent); }}

    .ctrl-actions {{ display: flex; align-items: center; gap: 10px; flex-wrap: wrap; }}
    .ctrl-actions .ctrl-run {{
      padding: 6px 14px;
      border-radius: 8px;
      background: var(--accent-soft);
      color: var(--link);
      font-weight: 600;
      font-size: 13px;
      border: 1px solid var(--border);
      cursor: pointer;
    }}
    .ctrl-actions .ctrl-run:hover {{ filter: brightness(0.97); }}
    .ctrl-actions .ctrl-run:disabled {{ opacity: 0.5; cursor: not-allowed; }}

    .dt-modal[hidden] {{ display: none !important; }}
    .dt-modal:not([hidden]) {{
      position: fixed; inset: 0; z-index: 3000;
      display: flex; align-items: center; justify-content: center;
      padding: 20px;
    }}
    .dt-modal-backdrop {{
      position: absolute; inset: 0; background: rgba(15, 23, 42, 0.5);
      backdrop-filter: blur(2px);
    }}
    .dt-modal-card {{
      position: relative; max-width: 420px; width: 100%;
      padding: 26px 24px 22px; border-radius: 16px;
      background: var(--card); border: 1px solid var(--border);
      box-shadow: 0 20px 50px rgba(0,0,0,0.2);
    }}
    .dt-modal-card h2 {{ margin: 0 0 12px 0; font-size: 1.2rem; letter-spacing: -0.02em; }}
    .dt-modal-card p {{ margin: 0 0 20px 0; color: var(--muted); font-size: 14px; line-height: 1.55; }}
    .dt-modal-card.dt-ok h2 {{ color: #15803d; }}
    .dt-modal-card.dt-bad h2 {{ color: #b91c1c; }}
    [data-theme="dark"] .dt-modal-card.dt-ok h2 {{ color: #4ade80; }}
    [data-theme="dark"] .dt-modal-card.dt-bad h2 {{ color: #f87171; }}
  </style>
</head>
<body>
  <div class="wrap">
  <div class="topbar">
    <div class="brand">
      <h1>DriverTranslator</h1>
      <span>Status &amp; controls · full page reload every 5s while this tab is visible</span>
    </div>
    <button class="btn" id="themeBtn" type="button">Theme</button>
  </div>

  <div class="section-title">Overview</div>
  <div class="card">
    <div class="row"><div>Uptime</div><div><code>{uptime_h}</code></div></div>
    <div class="row"><div>Mode</div><div><code>{snapshot['mode']}</code></div></div>
    <div class="row"><div>RTI clients</div><div><code>{snapshot['rti_clients']}</code></div></div>
    <div class="row"><div>Configured TX</div><div><code id="st_tx_configured">{snapshot['tx_configured']}</code></div></div>
    <div class="row"><div>Configured RX</div><div><code id="st_rx_configured">{snapshot['rx_configured']}</code></div></div>
    <div class="row"><div>AMX connections</div><div><code>{amx_conn}</code></div></div>
  </div>

  <div class="links-bar">
    <a href="/status.json">status.json</a>
    <a href="/logs.json">logs.json</a>
    <a href="/control.json">control.json</a>
  </div>

  <div class="section-title">Controls</div>
  <p class="subtle">Control changes are saved to config and survive restart/reboot. Hover <span class="help-icon" style="cursor:default" title="Each control has a ? with full help.">?</span> for details. Results open in a short on-page message.</p>
  <div class="card">
    <div class="row">
      <div><b>AMX dry-run mode</b><span class="help-icon" title="When ON, no AMX TCP connections are made and decoder state is simulated. Saved to config; restart required to apply.">?</span></div>
      <div class="ctrl-actions">
        <code id="st_amx_dry_run">{str(rt.get('amx_dry_run', cfg.amx_dry_run)).lower()}</code>
        <button type="button" class="ctrl-run" data-dt-ctl="set" data-key="amx_dry_run" data-value="{'false' if rt.get('amx_dry_run', cfg.amx_dry_run) else 'true'}">Toggle</button>
      </div>
    </div>
    <div class="row">
      <div><b>AMX persistent mode</b><span class="help-icon" title="When ON, keep a socket open per decoder. When OFF, use connect-close per command. Saved to config; restart required to apply.">?</span></div>
      <div class="ctrl-actions">
        <code id="st_amx_persistent">{str(rt.get('amx_persistent', cfg.amx_persistent)).lower()}</code>
        <button type="button" class="ctrl-run" data-dt-ctl="set" data-key="amx_persistent" data-value="{'false' if rt.get('amx_persistent', cfg.amx_persistent) else 'true'}">Toggle</button>
      </div>
    </div>
    <div class="row">
      <div><b>AMX verify after switch</b><span class="help-icon" title="When ON, after each route the translator asks each affected decoder for STREAM via AMX and logs mismatches locally. RTI still gets an immediate matrix ack.">?</span></div>
      <div class="ctrl-actions">
        <code id="st_amx_verify">{str(rt['amx_verify_after_set']).lower()}</code>
        <button type="button" class="ctrl-run" data-dt-ctl="set" data-key="amx_verify_after_set" data-value="{'false' if rt['amx_verify_after_set'] else 'true'}">Toggle</button>
      </div>
    </div>
    <div class="row">
      <div><b>AMX verify timeout</b><span class="help-icon" title="How long to wait (ms) for each decoder status read during verify. Lower is snappier; raise if decoders are slow or the network is busy.">?</span></div>
      <div class="ctrl-actions">
        <code id="st_verify_ms">{rt['amx_verify_timeout_ms']} ms</code>
        <input id="verifyTo" class="btn" style="width:88px; padding:6px 8px;" type="number" min="100" max="5000" step="50" value="{rt['amx_verify_timeout_ms']}"/>
        <button id="applyVerifyTo" class="btn btn-primary ctrl-run" type="button" data-dt-ctl="verify_ms">Apply</button>
      </div>
    </div>
    <div class="row">
      <div><b>Expanded log</b><span class="help-icon" title="When ON, logs each line sent back to RTI as 'RTI <- ...' in addition to incoming RTI lines. Runtime only.">?</span></div>
      <div class="ctrl-actions">
        <code id="st_expanded_log">{str(rt.get('expanded_log', False)).lower()}</code>
        <button type="button" class="ctrl-run" data-dt-ctl="set" data-key="expanded_log" data-value="{'false' if rt.get('expanded_log', False) else 'true'}">Toggle</button>
      </div>
    </div>
    <div class="row">
      <div><b>System size (TX/RX + starting IPs)</b><span class="help-icon" title="Regenerates endpoints.tx/endpoints.rx using installer naming and sequential IPs. TX uses INn-BOXn, stream=n, hostname NHD-120-TX-000...n. RX uses OUTn-TVn, hostname NHD-120-RX-000...(100+n), and amx_decoder_ip = RX IP. Saved to config; restart required.">?</span></div>
      <div class="ctrl-actions">
        <label class="subtle" for="txCount">TX</label>
        <input id="txCount" class="btn" style="width:72px; padding:6px 8px;" type="number" min="1" max="512" step="1" value="{snapshot['tx_configured']}"/>
        <label class="subtle" for="txStartIp">TX start IP</label>
        <input id="txStartIp" class="btn" style="width:140px; padding:6px 8px;" type="text" value="{html.escape(tx_start_ip)}"/>
        <label class="subtle" for="rxCount">RX</label>
        <input id="rxCount" class="btn" style="width:72px; padding:6px 8px;" type="number" min="1" max="512" step="1" value="{snapshot['rx_configured']}"/>
        <label class="subtle" for="rxStartIp">RX start IP</label>
        <input id="rxStartIp" class="btn" style="width:140px; padding:6px 8px;" type="text" value="{html.escape(rx_start_ip)}"/>
        <button id="applyEndpointSizing" class="btn btn-primary ctrl-run" type="button" data-dt-ctl="set_endpoints">Apply</button>
      </div>
    </div>
    <div class="row">
      <div><b>AMX self-test</b><span class="help-icon" title="Short TCP connect to every configured decoder IP. In dry-run mode all decoders count as reachable without connecting.">?</span></div>
      <div class="ctrl-actions"><button type="button" class="ctrl-run" data-dt-ctl="selftest">Run now</button></div>
    </div>
    <div class="row">
      <div><b>Restart DriverTranslator</b><span class="help-icon" title="Restarts only the DriverTranslator service (systemctl restart drivertranslator). Brief control interruption expected.">?</span></div>
      <div class="ctrl-actions"><button type="button" class="ctrl-run" data-dt-ctl="restart">Restart</button></div>
    </div>
    <div class="row">
      <div><b>Reboot host</b><span class="help-icon" title="Reboots this Linux machine (systemctl reboot). SSH and this page drop until the system is back.">?</span></div>
      <div class="ctrl-actions"><button type="button" class="ctrl-run" data-dt-ctl="reboot">Reboot</button></div>
    </div>
  </div>

  <div class="section-title">Matrix</div>
  <p class="subtle">Combined TX/RX matrix view. TX rows are polled from AMX <code>getStatus</code> every {_TX_STATUS_POLL_INTERVAL_SECONDS}s. RX rows show routed source and HDMI output (<code>HDMIOFF</code>: <code>ON</code>=enabled, <code>OFF</code>=disabled). Skip toggles are saved to config and apply after restart.</p>
  <div class="table-wrap">
  <table>
    <thead><tr><th>Endpoint</th><th>Route / Stream</th><th>Status</th><th>Signal</th><th>Skip</th></tr></thead>
    <tbody id="matrixBody">
      {route_html}
    </tbody>
  </table>
  </div>

  <div class="section-title">Recent logs</div>
  <pre id="logsPre">{log_lines}</pre>

  <div class="section-title">Unrecognized RTI commands</div>
  <p class="subtle">Lines the WyreStorm driver sent on the <b>RTI TCP port</b> (e.g. 2323) that returned <code>unknown command</code> (or similar). Identical lines are merged; the number is how many times each was sent. Times are <b>UTC</b>. Select the box below and copy for support. {_uc_persist_note}</p>
  <pre id="unknownCtlPre" style="max-height:320px;overflow-y:auto;font-size:11px;">{_unknown_pre}</pre>
  <div class="unk-ctl-actions">
    <button type="button" class="ctrl-run" data-dt-ctl="copy_unknown_ctl">Copy all</button>
    <button type="button" class="ctrl-run" data-dt-ctl="clear_unknown_ctl">Clear unrecognized list</button>
    <span class="subtle" style="display:block;margin-top:8px;margin-bottom:0;">Wipes this list and the on-disk file (when persistence is enabled). Page reloads after confirm.</span>
  </div>

  <div id="dtModal" class="dt-modal" hidden>
    <div class="dt-modal-backdrop" id="dtModalBackdrop"></div>
    <div class="dt-modal-card" id="dtModalCard" role="dialog" aria-modal="true" aria-labelledby="dtModalTitle">
      <h2 id="dtModalTitle"></h2>
      <p id="dtModalText"></p>
      <button type="button" class="btn btn-primary" id="dtModalOk">OK</button>
    </div>
  </div>
  </div>
  <script>
    (function () {{
      const DT_UI = {{ sess: {_ui_sess_js}, ctl: {_ctl_qs_js}, port: {_amx_port} }};
      function ctlUrl(path) {{
        const sep = path.indexOf('?') >= 0 ? '&' : '?';
        let u = path + sep + 'ui_sess=' + encodeURIComponent(DT_UI.sess);
        if (DT_UI.ctl) u += DT_UI.ctl;
        return u;
      }}

      const modal = document.getElementById('dtModal');
      const modalCard = document.getElementById('dtModalCard');
      const modalTitle = document.getElementById('dtModalTitle');
      const modalText = document.getElementById('dtModalText');
      const modalOk = document.getElementById('dtModalOk');
      const modalBackdrop = document.getElementById('dtModalBackdrop');

      function showModal(ok, title, text) {{
        modalCard.classList.remove('dt-ok', 'dt-bad');
        modalCard.classList.add(ok ? 'dt-ok' : 'dt-bad');
        modalTitle.textContent = title;
        modalText.textContent = text;
        modal.removeAttribute('hidden');
      }}
      function hideModal() {{ modal.setAttribute('hidden', ''); }}
      modalOk.addEventListener('click', hideModal);
      modalBackdrop.addEventListener('click', hideModal);
      document.addEventListener('keydown', (e) => {{ if (e.key === 'Escape') hideModal(); }});

      function getCtrlButtons() {{
        return document.querySelectorAll('.ctrl-run');
      }}
      let dtBusy = false;
      function setBusy(on) {{
        dtBusy = !!on;
        getCtrlButtons().forEach((b) => {{ b.disabled = !!on; }});
      }}

      function setSavedMessage(key, j) {{
        if (key === 'amx_dry_run')
          return 'AMX dry-run mode is now ' + String(j.amx_dry_run).toLowerCase() + '. Saved to config; restart DriverTranslator to apply.';
        if (key === 'amx_persistent')
          return 'AMX persistent mode is now ' + String(j.amx_persistent).toLowerCase() + '. Saved to config; restart DriverTranslator to apply.';
        if (key === 'amx_verify_timeout_ms')
          return 'Verify timeout is now ' + j.amx_verify_timeout_ms + ' ms. Applies immediately; no restart.';
        if (key === 'amx_verify_after_set')
          return 'AMX verify after switch is now ' + String(j.amx_verify_after_set).toLowerCase() + '. Applies immediately.';
        if (key === 'expanded_log')
          return 'Expanded log is now ' + String(j.expanded_log).toLowerCase() + '. Applies immediately.';
        return 'Setting updated. Applies immediately.';
      }}

      async function handleControlResponse(r, okTitle, getDetail) {{
        if (r.status === 401) {{
          showModal(false, 'Session expired', 'Refresh this page and sign in again, then retry.');
          return;
        }}
        if (r.status === 403) {{
          showModal(false, 'Not allowed', 'Wrong or missing control token in config. Check http_status.control_token.');
          return;
        }}
        const t = await r.text();
        let j = null;
        try {{ j = JSON.parse(t); }} catch (e) {{}}
        if (!r.ok) {{
          showModal(false, 'Failed', (j && j.error) ? j.error : (t.slice(0, 200) || r.status + ' ' + r.statusText));
          return;
        }}
        showModal(true, okTitle, getDetail(j, t));
      }}

      document.querySelectorAll('[data-dt-ctl="set"]').forEach((btn) => {{
        btn.addEventListener('click', async () => {{
          const key = btn.getAttribute('data-key');
          const value = btn.getAttribute('data-value');
          setBusy(true);
          try {{
            const url = ctlUrl('/control/set?key=' + encodeURIComponent(key) + '&value=' + encodeURIComponent(value));
            const r = await fetch(url);
            await handleControlResponse(r, 'Saved', (j) => {{
              if (!j) return 'Done.';
              const msg = setSavedMessage(key, j);
              if (key === 'amx_verify_after_set') {{
                const el = document.getElementById('st_amx_verify');
                if (el) el.textContent = String(j.amx_verify_after_set).toLowerCase();
                btn.setAttribute('data-value', j.amx_verify_after_set ? 'false' : 'true');
              }}
              if (key === 'amx_dry_run') {{
                const el = document.getElementById('st_amx_dry_run');
                if (el) el.textContent = String(j.amx_dry_run).toLowerCase();
                btn.setAttribute('data-value', j.amx_dry_run ? 'false' : 'true');
              }}
              if (key === 'amx_persistent') {{
                const el = document.getElementById('st_amx_persistent');
                if (el) el.textContent = String(j.amx_persistent).toLowerCase();
                btn.setAttribute('data-value', j.amx_persistent ? 'false' : 'true');
              }}
              if (key === 'expanded_log') {{
                const el = document.getElementById('st_expanded_log');
                if (el) el.textContent = String(j.expanded_log).toLowerCase();
                btn.setAttribute('data-value', j.expanded_log ? 'false' : 'true');
              }}
              return msg;
            }});
          }} catch (e) {{
            showModal(false, 'Network error', String(e.message || e));
          }} finally {{
            setBusy(false);
          }}
        }});
      }});

      const applyVerifyTo = document.getElementById('applyVerifyTo');
      const verifyTo = document.getElementById('verifyTo');
      if (applyVerifyTo && verifyTo) {{
        applyVerifyTo.addEventListener('click', async () => {{
          const v = String(Math.max(100, Math.min(5000, parseInt(verifyTo.value || '800', 10) || 800)));
          verifyTo.value = v;
          setBusy(true);
          try {{
            const r = await fetch(ctlUrl('/control/set?key=amx_verify_timeout_ms&value=' + encodeURIComponent(v)));
            await handleControlResponse(r, 'Saved', (j) => {{
              if (j && j.amx_verify_timeout_ms != null) {{
                const el = document.getElementById('st_verify_ms');
                if (el) el.textContent = j.amx_verify_timeout_ms + ' ms';
              }}
              return setSavedMessage('amx_verify_timeout_ms', j || {{}});
            }});
          }} catch (e) {{
            showModal(false, 'Network error', String(e.message || e));
          }} finally {{
            setBusy(false);
          }}
        }});
      }}

      const applyEndpointSizing = document.getElementById('applyEndpointSizing');
      const txCount = document.getElementById('txCount');
      const rxCount = document.getElementById('rxCount');
      const txStartIp = document.getElementById('txStartIp');
      const rxStartIp = document.getElementById('rxStartIp');
      if (applyEndpointSizing && txCount && rxCount && txStartIp && rxStartIp) {{
        applyEndpointSizing.addEventListener('click', async () => {{
          const txc = String(Math.max(1, Math.min(512, parseInt(txCount.value || '1', 10) || 1)));
          const rxc = String(Math.max(1, Math.min(512, parseInt(rxCount.value || '1', 10) || 1)));
          txCount.value = txc;
          rxCount.value = rxc;
          const txip = String(txStartIp.value || '').trim();
          const rxip = String(rxStartIp.value || '').trim();
          if (!txip || !rxip) {{
            showModal(false, 'Missing values', 'Enter both TX and RX starting IP addresses.');
            return;
          }}
          if (!confirm('Save new TX/RX sizing and starting IP ranges to config? DriverTranslator restart is required to apply.')) return;
          setBusy(true);
          try {{
            const u = ctlUrl(
              '/control/set_endpoints?tx_count=' + encodeURIComponent(txc) +
              '&rx_count=' + encodeURIComponent(rxc) +
              '&tx_start_ip=' + encodeURIComponent(txip) +
              '&rx_start_ip=' + encodeURIComponent(rxip)
            );
            const r = await fetch(u);
            await handleControlResponse(r, 'Endpoint sizing saved', (j) => {{
              if (j) {{
                const txEl = document.getElementById('st_tx_configured');
                const rxEl = document.getElementById('st_rx_configured');
                if (txEl && j.tx_count != null) txEl.textContent = String(j.tx_count);
                if (rxEl && j.rx_count != null) rxEl.textContent = String(j.rx_count);
                if (j.tx_start_ip) txStartIp.value = String(j.tx_start_ip);
                if (j.rx_start_ip) rxStartIp.value = String(j.rx_start_ip);
              }}
              return 'Saved to config. Restart DriverTranslator to apply the new endpoint lists.';
            }});
          }} catch (e) {{
            showModal(false, 'Network error', String(e.message || e));
          }} finally {{
            setBusy(false);
          }}
        }});
      }}

      function bindEndpointSkipButtons(scope) {{
        (scope || document).querySelectorAll('[data-dt-ctl="set_endpoint_skip"]').forEach((btn) => {{
          if (btn.getAttribute('data-dt-bound-skip') === '1') return;
          btn.setAttribute('data-dt-bound-skip', '1');
          btn.addEventListener('click', async () => {{
          const kind = String(btn.getAttribute('data-kind') || '').toLowerCase();
          const alias = String(btn.getAttribute('data-alias') || '');
          const skip = String(btn.getAttribute('data-skip') || 'true').toLowerCase();
          if (!kind || !alias) {{
            showModal(false, 'Invalid endpoint', 'Missing endpoint kind or alias.');
            return;
          }}
          const action = (skip === 'true') ? 'skip' : 'unskip';
          if (!confirm('Save and ' + action + ' ' + alias + '? Restart DriverTranslator to apply.')) return;
          setBusy(true);
          try {{
            const u = ctlUrl(
              '/control/set_endpoint_skip?kind=' + encodeURIComponent(kind) +
              '&alias=' + encodeURIComponent(alias) +
              '&skip=' + encodeURIComponent(skip)
            );
            const r = await fetch(u);
            await handleControlResponse(r, 'Endpoint updated', (j) => {{
              if (!j) return 'Saved to config. Restart DriverTranslator to apply.';
              return String(j.kind || '').toUpperCase() + ' ' + String(j.alias || alias) +
                ' skip is now ' + String(j.skip).toLowerCase() + '. Restart DriverTranslator to apply.';
            }});
            await refreshLiveSections();
          }} catch (e) {{
            showModal(false, 'Network error', String(e.message || e));
          }} finally {{
            setBusy(false);
          }}
          }});
        }});
      }}
      bindEndpointSkipButtons(document);

      document.querySelectorAll('[data-dt-ctl="selftest"]').forEach((btn) => {{
        btn.addEventListener('click', async () => {{
          setBusy(true);
          try {{
            const r = await fetch(ctlUrl('/control/selftest'));
            if (r.status === 401) {{
              showModal(false, 'Session expired', 'Refresh this page and sign in again, then retry.');
              return;
            }}
            if (r.status === 403) {{
              showModal(false, 'Not allowed', 'Wrong or missing control token in config.');
              return;
            }}
            const t = await r.text();
            let j = null;
            try {{ j = JSON.parse(t); }} catch (e) {{}}
            if (!r.ok) {{
              showModal(false, 'Self-test failed', (t || r.statusText).slice(0, 300));
              return;
            }}
            const total = (j && j.total) | 0, okn = (j && j.ok) | 0;
            const allOk = !total || okn === total;
            let detail = 'Done.';
            if (j) {{
              if (!total) detail = 'No decoders are configured.';
              else {{
                const unr = j.unreachable || [];
                if (unr.length)
                  detail = okn + ' of ' + total + ' decoders reachable on port ' + DT_UI.port + '. Unreachable: ' + unr.join(', ') + '.';
                else
                  detail = 'All ' + total + ' decoders accepted a TCP connection on port ' + DT_UI.port + '.';
              }}
            }}
            showModal(allOk, allOk ? 'Self-test passed' : 'Self-test: issues found', detail);
          }} catch (e) {{
            showModal(false, 'Network error', String(e.message || e));
          }} finally {{
            setBusy(false);
          }}
        }});
      }});

      document.querySelectorAll('[data-dt-ctl="clear_unknown_ctl"]').forEach((btn) => {{
        btn.addEventListener('click', async () => {{
          if (!confirm('Clear the unrecognized-command list? This cannot be undone.')) return;
          setBusy(true);
          try {{
            const r = await fetch(ctlUrl('/control/clear_unknown_ctl'));
            if (r.status === 401) {{
              showModal(false, 'Session expired', 'Refresh this page and sign in again, then retry.');
              return;
            }}
            if (r.status === 403) {{
              showModal(false, 'Not allowed', 'Wrong or missing control token in config.');
              return;
            }}
            const t = await r.text();
            if (!r.ok) {{
              let j = null;
              try {{ j = JSON.parse(t); }} catch (e) {{}}
              showModal(false, 'Failed', (j && j.error) ? j.error : (t.slice(0, 200) || r.statusText));
              return;
            }}
            location.reload();
          }} catch (e) {{
            showModal(false, 'Network error', String(e.message || e));
          }} finally {{
            setBusy(false);
          }}
        }});
      }});

      document.querySelectorAll('[data-dt-ctl="copy_unknown_ctl"]').forEach((btn) => {{
        btn.addEventListener('click', async () => {{
          const pre = document.getElementById('unknownCtlPre');
          if (!pre) {{
            showModal(false, 'Not found', 'Unknown command list box is missing.');
            return;
          }}
          const text = (pre.textContent || '').replace(/\u00a0/g, ' ').trimEnd();
          if (!text) {{
            showModal(true, 'Copied', 'List is empty.');
            return;
          }}
          try {{
            if (navigator.clipboard && window.isSecureContext) {{
              await navigator.clipboard.writeText(text);
              showModal(true, 'Copied', 'Copied full unrecognized-command list to clipboard.');
              return;
            }}
            // Fallback for HTTP/non-secure contexts where Clipboard API is blocked.
            const ta = document.createElement('textarea');
            ta.value = text;
            ta.setAttribute('readonly', '');
            ta.style.position = 'fixed';
            ta.style.left = '-9999px';
            ta.style.top = '0';
            document.body.appendChild(ta);
            ta.focus();
            ta.select();
            const ok = document.execCommand('copy');
            document.body.removeChild(ta);
            if (!ok) throw new Error('execCommand copy failed');
            showModal(true, 'Copied', 'Copied full unrecognized-command list to clipboard.');
          }} catch (e) {{
            showModal(false, 'Copy failed', 'Browser blocked clipboard access. Select the box and copy manually.');
          }}
        }});
      }});

      document.querySelectorAll('[data-dt-ctl="reboot"]').forEach((btn) => {{
        btn.addEventListener('click', async () => {{
          if (!confirm('Reboot this machine? SSH and this page will disconnect until it is back.')) return;
          setBusy(true);
          try {{
            const r = await fetch(ctlUrl('/control/reboot'));
            await handleControlResponse(r, 'Reboot scheduled', () =>
              'The system will restart shortly. Reopen this page after boot if needed.');
          }} catch (e) {{
            showModal(false, 'Network error', String(e.message || e));
          }} finally {{
            setBusy(false);
          }}
        }});
      }});

      document.querySelectorAll('[data-dt-ctl="restart"]').forEach((btn) => {{
        btn.addEventListener('click', async () => {{
          if (!confirm('Restart DriverTranslator service now? Control may drop briefly.')) return;
          setBusy(true);
          try {{
            const r = await fetch(ctlUrl('/control/restart'));
            await handleControlResponse(r, 'Service restart scheduled', () =>
              'DriverTranslator will restart shortly. Reload this page if it disconnects.');
          }} catch (e) {{
            showModal(false, 'Network error', String(e.message || e));
          }} finally {{
            setBusy(false);
          }}
        }});
      }});

      const themeBtn = document.getElementById('themeBtn');
      const root = document.documentElement;
      const themeKey = 'dt_theme';
      function applyTheme(theme) {{
        if (!theme) {{ root.removeAttribute('data-theme'); return; }}
        root.setAttribute('data-theme', theme);
      }}
      const savedTheme = localStorage.getItem(themeKey);
      if (savedTheme === 'light' || savedTheme === 'dark') applyTheme(savedTheme);
      themeBtn.addEventListener('click', () => {{
        const current = root.getAttribute('data-theme');
        const next = current === 'dark' ? 'light' : 'dark';
        applyTheme(next);
        localStorage.setItem(themeKey, next);
      }});

      const matrixBodyEl = document.getElementById('matrixBody');
      const logsPreEl = document.getElementById('logsPre');
      const unknownCtlPreEl = document.getElementById('unknownCtlPre');
      let refreshInFlight = false;
      async function refreshLiveSections() {{
        if (dtBusy || refreshInFlight || document.hidden) return;
        refreshInFlight = true;
        try {{
          const r = await fetch(window.location.pathname + window.location.search, {{ cache: 'no-store' }});
          if (!r.ok) return;
          const t = await r.text();
          const doc = new DOMParser().parseFromString(t, 'text/html');
          const newMatrixBody = doc.getElementById('matrixBody');
          const newLogsPre = doc.getElementById('logsPre');
          const newUnknownPre = doc.getElementById('unknownCtlPre');
          if (matrixBodyEl && newMatrixBody) {{
            matrixBodyEl.innerHTML = newMatrixBody.innerHTML;
            bindEndpointSkipButtons(matrixBodyEl);
          }}
          if (logsPreEl && newLogsPre) logsPreEl.textContent = newLogsPre.textContent || '';
          if (unknownCtlPreEl && newUnknownPre) unknownCtlPreEl.textContent = newUnknownPre.textContent || '';
        }} catch (_e) {{
          // keep page usable on transient refresh errors
        }} finally {{
          refreshInFlight = false;
        }}
      }}

      setInterval(function () {{
        refreshLiveSections();
      }}, 5000);
    }})();
  </script>
</body>
</html>
""".encode("utf-8")
            writer.write(_http_response("200 OK", "text/html; charset=utf-8", body))
            return

        writer.write(_http_response("404 Not Found", "text/plain", b"not found"))
    finally:
        with contextlib.suppress(Exception):
            await writer.drain()
        writer.close()
        with contextlib.suppress(Exception):
            await writer.wait_closed()


class _RtiControlUdp(asyncio.DatagramProtocol):
    def __init__(self, *, cfg: Config):
        self._cfg = cfg
        self._last_reboot_at = 0.0

    def datagram_received(self, data: bytes, addr) -> None:  # type: ignore[override]
        if not self._cfg.rti_control_enabled:
            return
        text = data.decode("utf-8", errors="replace").strip()
        if not text:
            return
        want = (self._cfg.rti_control_reboot_command or "reboot").strip()
        if text.lower() != want.lower():
            return

        now = time.monotonic()
        if (now - self._last_reboot_at) < 60:
            LOG.warning("RTI control reboot suppressed (cooldown)")
            return
        self._last_reboot_at = now
        LOG.error("RTI control requested reboot from %s", addr)
        asyncio.create_task(_do_reboot(reason=f"rti_udp:{addr}"))


# ---------------------------------------------------------------------------
# RTI/NHD-CTL protocol helpers and command surface
# ---------------------------------------------------------------------------
def _handle_config_get(cfg: Config, session: NhdCtlSession, state: ControllerState, cmd: str) -> List[str]:
    parts = cmd.split()
    if parts[:3] == ["config", "get", "version"]:
        return [f"API version: v{cfg.nhd.api} System version: v{cfg.nhd.web}(v{cfg.nhd.core})"]

    if parts[:3] == ["config", "get", "ipsetting"]:
        ip = cfg.nhd.ipsetting
        return [f"ipsetting is:static {ip['ip4addr']} {ip['netmask']} {ip['gateway']}"]

    if parts[:3] == ["config", "get", "ipsetting2"]:
        ip = cfg.nhd.ipsetting2
        return [f"ipsetting2 is:static {ip['ip4addr']} {ip['netmask']} {ip['gateway']}"]

    if parts[:3] == ["config", "get", "newip4addr"]:
        ip = cfg.nhd.ipsetting
        return [f"ipsetting is:static {ip['ip4addr']} {ip['netmask']} {ip['gateway']}"]

    if parts[:3] == ["config", "get", "newip4addr2"]:
        ip = cfg.nhd.ipsetting2
        return [f"ipsetting2 is:static {ip['ip4addr']} {ip['netmask']} {ip['gateway']}"]

    if parts[:3] == ["config", "get", "telnet"] and len(parts) >= 4 and parts[3] == "alias":
        return ["telnet alias is on" if session.alias_mode else "telnet alias is off"]

    if parts[:3] == ["config", "get", "rs-232"] and len(parts) >= 4 and parts[3] == "alias":
        return ["rs-232 alias is on" if session.alias_mode else "rs-232 alias is off"]

    if parts[:3] == ["config", "get", "htmlLog"]:
        return ["htmlLog true"]

    if parts[:4] == ["config", "get", "userList"]:
        return ["userList null null null null null"]

    if parts[:4] == ["config", "get", "dnsserver"] and len(parts) >= 5:
        if parts[4] == "ip4addr":
            return ["dns server ip4addr is: 8.8.8.8"]
        if parts[4] == "ip4addr2":
            return ["dns server ip4addr2 is: 1.1.1.1"]

    if parts[:4] == ["config", "get", "controller", "info"]:
        info = {
            "hostname_av": cfg.nhd.ipsetting.get("ip4addr", "NHD-CTL-AV"),
            "hostname_ctl": cfg.nhd.ipsetting2.get("ip4addr", "NHD-CTL-CTL"),
            "ip_av": cfg.nhd.ipsetting.get("ip4addr", "0.0.0.0"),
            "ip_ctl": cfg.nhd.ipsetting2.get("ip4addr", "0.0.0.0"),
            "mac_av": "00:00:00:00:00:00",
            "mac_ctl": "00:00:00:00:00:01",
            "serialNumber": "00000000000000",
            "version": f"V{cfg.nhd.web}",
        }
        return ["controller info: " + _ctl_json([info])]

    if parts[:4] == ["config", "get", "service", "capability"] and len(parts) >= 5:
        cap = parts[4]
        val = {
            "service_https": "false",
            "service_http": "true",
            "service_sshapi": "true",
            "service_telnettlsapi": "true",
            "service_telnetapi": "true",
        }.get(cap)
        if val is not None:
            return [f"config get service capability {cap} {val}"]

    if parts[:4] == ["config", "get", "system", "sshservice"]:
        return ["system sshservice is on"]

    if parts[:5] == ["config", "get", "system", "service", "ssh_api"] and len(parts) >= 7 and parts[6] == "port":
        return ["system service ssh_api port is 10022"]
    if parts[:5] == ["config", "get", "system", "service", "telnettls_api"] and len(parts) >= 7 and parts[6] == "port":
        return ["system service telnettls_api port is 992"]
    if parts[:5] == ["config", "get", "system", "service", "telnet_api"] and len(parts) >= 7 and parts[6] == "port":
        return ["system service telnet_api port is 23"]

    if parts[:4] == ["config", "get", "system", "realtime"]:
        return [f"RealTime:{time.strftime('%a,%d-%m-%Y,%H:%M:%S', time.gmtime())}"]
    if parts[:4] == ["config", "get", "system", "ntpserverstatus"]:
        return ["system ntpserverstatus is unreachable"]
    if parts[:4] == ["config", "get", "system", "ntpzone"]:
        return ["system ntpzone is Etc UTC"]
    if parts[:4] == ["config", "get", "system", "ntpenable"]:
        return ["system ntpenable is off"]
    if parts[:4] == ["config", "get", "system", "ntpserver"]:
        return ["system ntpserver is 0.pool.ntp.org"]
    if parts[:4] == ["config", "get", "system", "web_logout_time"]:
        return ["system web_logout_time is 1440"]
    if parts[:4] == ["config", "get", "system", "preview"] and len(parts) >= 6 and parts[5] == "fps":
        return ["system preview fps: 0"]
    if parts[:4] == ["config", "get", "system", "lcd"]:
        return ["system lcd:ipversion"]
    if parts[:4] == ["config", "get", "system", "xyte_setting"]:
        return ["xyte_setting info: " + _ctl_json({"xyte_setting": {"enable": True, "register_url": "https://entry.xyte.io"}})]
    if parts[:4] == ["config", "get", "system", "xyte_status"]:
        return ["xyte status: " + _ctl_json({"cloud_status": "connected", "register_status": "registered"})]
    if parts[:4] == ["config", "get", "system", "802_1x"]:
        return [
            "802_1x info: "
            + _ctl_json(
                [
                    {
                        "ieee802_1x_ca_mode": "default",
                        "ieee802_1x_enable": "false",
                        "ieee802_1x_mode": "",
                        "ieee802_1x_mschapv2_password": "",
                        "ieee802_1x_mschapv2_user": "",
                        "ieee802_1x_tls_private_key_password": "",
                        "ieee802_1x_tls_user": "",
                    }
                ]
            )
        ]
    if parts[:4] == ["config", "get", "system", "ldap"]:
        return [
            "ldap info: "
            + _ctl_json(
                [
                    {
                        "ldap_attr": "",
                        "ldap_base_dn": "",
                        "ldap_bind_dn": "",
                        "ldap_enable": "false",
                        "ldap_mode": "dn",
                        "ldap_password": "",
                        "ldap_uid": "",
                        "ldap_uri": "",
                    }
                ]
            )
        ]

    if parts[:3] == ["config", "get", "devicelist"]:
        # Doc: only online devices returned. We treat all configured devices as online.
        names = _all_endpoint_aliases(cfg)
        return ["devicelist is " + " ".join(names)]

    if parts[:3] == ["config", "get", "name"]:
        # `config get name` (all), or `config get name <aliasOrHostname>`
        if len(parts) == 3:
            lines: List[str] = []
            for r in cfg.rx_by_alias.values():
                lines.append(f"{r.hostname}'s alias is {r.alias}")
            for t in cfg.tx_by_alias.values():
                lines.append(f"{t.hostname}'s alias is {t.alias}")
            return lines

        token = parts[3]
        tx = _lookup_tx(cfg, token)
        if tx is not None:
            return [f"{tx.hostname}'s alias is {tx.alias}"]
        rx = _lookup_rx(cfg, token)
        if rx is not None:
            return [f"{rx.hostname}'s alias is {rx.alias}"]
        return ["unknown command"]

    if parts[:3] == ["config", "get", "devicejsonstring"]:
        devices: List[Dict[str, Any]] = []
        for t in cfg.tx_by_alias.values():
            devices.append(
                {
                    "aliasName": t.alias,
                    "deviceType": "Transmitter",
                    "trueName": t.hostname,
                    "name": t.hostname,
                    "online": True,
                }
            )
        for r in cfg.rx_by_alias.values():
            tx_alias = state.video.get(r.alias)
            tx_obj = cfg.tx_by_alias.get(tx_alias) if tx_alias else None
            devices.append(
                {
                    "aliasName": r.alias,
                    "deviceType": "Receiver",
                    "trueName": r.hostname,
                    "name": r.hostname,
                    "txName": tx_obj.hostname if tx_obj is not None else "NULL",
                    "online": bool(state.rx_online.get(r.alias, True)),
                }
            )
        return ["device json string:" + _ctl_json(devices)]

    if parts[:4] == ["config", "get", "device", "info"]:
        devices: List[Dict[str, Any]] = []
        if len(parts) == 4:
            for t in cfg.tx_by_alias.values():
                devices.append({"aliasname": t.alias, "name": t.hostname, "devicetype": "Transmitter"})
            for r in cfg.rx_by_alias.values():
                devices.append({"aliasname": r.alias, "name": r.hostname, "devicetype": "Receiver"})
        else:
            for token in parts[4:]:
                tx = _lookup_tx(cfg, token)
                if tx is not None:
                    devices.append({"aliasname": tx.alias, "name": tx.hostname, "devicetype": "Transmitter"})
                    continue
                rx = _lookup_rx(cfg, token)
                if rx is not None:
                    devices.append({"aliasname": rx.alias, "name": rx.hostname, "devicetype": "Receiver"})
            if not devices:
                return ["unknown command"]
        return ["devices json info: " + _ctl_json({"devices": devices})]

    # Section 13.2 — device real-time status (100/110/140/200-tier JSON shape, API v6.6 / Appendix-style).
    if parts[:4] == ["config", "get", "device", "status"]:
        tok = parts[4:]

        def _pack(rows: List[Dict[str, str]]) -> List[str]:
            body = {"devices status": rows}
            return ["devices status info: " + _ctl_json(body)]

        if len(tok) == 0:
            rows: List[Dict[str, str]] = []
            for t in cfg.tx_by_alias.values():
                rows.append(_device_status_tx_dict(t))
            for r in cfg.rx_by_alias.values():
                rows.append(_device_status_rx_dict(r, state))
            return _pack(rows)

        rows: List[Dict[str, str]] = []
        for token in tok:
            tx = _lookup_tx(cfg, token)
            if tx is not None:
                rows.append(_device_status_tx_dict(tx))
                continue
            rx = _lookup_rx(cfg, token)
            if rx is not None:
                rows.append(_device_status_rx_dict(rx, state))
        if rows:
            return _pack(rows)

        return ["unknown command"]

    return ["unknown command"]


async def _handle_matrix_set(
    cfg: Config, amx: Any, state: ControllerState, cmd: str, timeout_ms: int
) -> Tuple[bool, str, List[Tuple[str, str, str]], Dict[str, Dict[str, str]]]:
    # cmd: "matrix set <TX> <RX1> <RX2> ... <RXn>"
    parts = cmd.split()
    if len(parts) < 4:
        return False, "unknown command", [], {}
    if parts[0].lower() != "matrix" or parts[1].lower() != "set":
        return False, "unknown command", [], {}

    tx_token = parts[2]
    rx_tokens = parts[3:]

    tx = _lookup_tx(cfg, tx_token)
    if tx is None:
        # Allow explicit NULL routing in WyreStorm API
        if tx_token.upper() == "NULL":
            tx = None
        else:
            return False, "unknown command", [], {}

    rxs: List[Rx] = []
    for token in rx_tokens:
        rx = _lookup_rx(cfg, token)
        if rx is None:
            return False, "unknown command", [], {}
        rxs.append(rx)

    failures: List[Tuple[str, str, str]] = []
    status_by_rx: Dict[str, Dict[str, str]] = {}
    if tx is not None:
        failures, status_by_rx = await _apply_amx_command_to_rx_aliases(
            cfg=cfg,
            amx=amx,
            state=state,
            rx_aliases=[rx.alias for rx in rxs],
            command=f"set:{tx.amx_stream}",
            timeout_ms=timeout_ms,
        )
    else:
        LOG.info("AMX routing skipped: NULL assignment requested")

    # WyreStorm ack is a "command mirror"
    return True, cmd, failures, status_by_rx


async def _refresh_hdmi_outputs(*, cfg: Config, amx: Any, state: ControllerState, timeout_ms: int) -> None:
    if not hasattr(amx, "get_hdmi_output"):
        return
    rx_aliases = sorted(
        [a for a in cfg.rx_by_alias.keys() if a not in cfg.rx_skipped_aliases],
        key=_rx_alias_sort_key,
    )
    if not rx_aliases:
        return
    results = await asyncio.gather(
        *(
            amx.get_hdmi_output(
                decoder_ip=cfg.rx_by_alias[rx_alias].amx_decoder_ip,
                timeout_ms=timeout_ms,
            )
            for rx_alias in rx_aliases
        ),
        return_exceptions=True,
    )
    for rx_alias, res in zip(rx_aliases, results):
        if isinstance(res, BaseException):
            state.set_rx_online(rx_alias, False)
            state.set_rx_hdmi_output(rx_alias, None)
        else:
            state.set_rx_hdmi_output(rx_alias, res if isinstance(res, bool) else None)


async def _refresh_hdmi_outputs_for_aliases(
    *,
    cfg: Config,
    amx: Any,
    state: ControllerState,
    timeout_ms: int,
    rx_aliases: List[str],
) -> None:
    if not hasattr(amx, "get_hdmi_output"):
        return
    wanted = [a for a in rx_aliases if a in cfg.rx_by_alias and a not in cfg.rx_skipped_aliases]
    if not wanted:
        return
    results = await asyncio.gather(
        *(
            amx.get_hdmi_output(
                decoder_ip=cfg.rx_by_alias[rx_alias].amx_decoder_ip,
                timeout_ms=timeout_ms,
            )
            for rx_alias in wanted
        ),
        return_exceptions=True,
    )
    for rx_alias, res in zip(wanted, results):
        if isinstance(res, BaseException):
            state.set_rx_online(rx_alias, False)
            state.set_rx_hdmi_output(rx_alias, None)
        else:
            state.set_rx_hdmi_output(rx_alias, res if isinstance(res, bool) else None)


async def _read_amx_status_fields_from_ip(
    *,
    host: str,
    port: int,
    connect_timeout_ms: int,
    timeout_ms: int,
    local_addr: Optional[Tuple[str, int]],
    expanded_log: bool,
) -> Dict[str, str]:
    reader: Optional[asyncio.StreamReader] = None
    writer: Optional[asyncio.StreamWriter] = None
    try:
        reader, writer = await _open_connection(
            host,
            port,
            timeout=max(0.2, connect_timeout_ms / 1000),
            local_addr=local_addr,
        )
        writer.write(b"?\r")
        await writer.drain()
        data = await asyncio.wait_for(reader.read(4096), timeout=max(0.2, timeout_ms / 1000))
        _log_amx_inbound(enabled=expanded_log, decoder_ip=host, decoder_port=port, data=data)
        return _parse_amx_status(data)
    finally:
        if writer is not None:
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()


async def _refresh_tx_statuses(*, cfg: Config, state: ControllerState, runtime: RuntimeSettings) -> None:
    tx_aliases = sorted(
        [a for a in cfg.tx_by_alias.keys() if a not in cfg.tx_skipped_aliases],
        key=_tx_alias_sort_key,
    )
    if not tx_aliases:
        return

    if cfg.amx_dry_run:
        for tx_alias in tx_aliases:
            tx = cfg.tx_by_alias[tx_alias]
            fields = {
                "STREAM": str(tx.amx_stream),
                "PLAYMODE": "live",
                "MUTE": "0",
                "HDMIINPUT": "connected",
                "INPUTRES": "1920x1080",
            }
            state.set_tx_online(tx_alias, True)
            state.set_tx_status_fields(tx_alias, fields)
        return

    local_addr = (cfg.amx_bind_address, 0) if cfg.amx_bind_address else None
    timeout_ms = max(200, min(5000, int(runtime.amx_verify_timeout_ms)))
    tasks = []
    for tx_alias in tx_aliases:
        tx = cfg.tx_by_alias[tx_alias]
        if not tx.ip:
            tasks.append(None)
            continue
        tasks.append(
            _read_amx_status_fields_from_ip(
                host=tx.ip,
                port=cfg.amx_decoder_port,
                connect_timeout_ms=cfg.amx_connect_timeout_ms,
                timeout_ms=timeout_ms,
                local_addr=local_addr,
                expanded_log=runtime.expanded_log,
            )
        )

    awaited = [t for t in tasks if t is not None]
    results: List[Any] = []
    if awaited:
        results = list(await asyncio.gather(*awaited, return_exceptions=True))
    idx = 0
    for tx_alias, task in zip(tx_aliases, tasks):
        if task is None:
            state.set_tx_online(tx_alias, False)
            state.set_tx_status_fields(tx_alias, {})
            continue
        res = results[idx]
        idx += 1
        if isinstance(res, BaseException):
            state.set_tx_online(tx_alias, False)
            state.set_tx_status_fields(tx_alias, {})
        else:
            fields = res if isinstance(res, dict) else {}
            state.set_tx_online(tx_alias, bool(fields))
            state.set_tx_status_fields(tx_alias, fields)


class TxStatusPoller:
    def __init__(self, *, cfg: Config, state: ControllerState, runtime: RuntimeSettings) -> None:
        self._cfg = cfg
        self._state = state
        self._runtime = runtime
        self._task: Optional[asyncio.Task[None]] = None

    async def start(self) -> None:
        await _refresh_tx_statuses(cfg=self._cfg, state=self._state, runtime=self._runtime)
        self._task = asyncio.create_task(self._loop(), name="dt-tx-status-poller")

    async def _loop(self) -> None:
        while True:
            await asyncio.sleep(_TX_STATUS_POLL_INTERVAL_SECONDS)
            try:
                await _refresh_tx_statuses(cfg=self._cfg, state=self._state, runtime=self._runtime)
            except Exception:
                LOG.exception("TX status poll failed")


# ---------------------------------------------------------------------------
# Routing helpers and RTI TCP client handler
# ---------------------------------------------------------------------------
async def _apply_amx_command_to_rx_aliases(
    *,
    cfg: Config,
    amx: Any,
    state: ControllerState,
    rx_aliases: List[str],
    command: str,
    timeout_ms: int,
) -> Tuple[List[Tuple[str, str, str]], Dict[str, Dict[str, str]]]:
    rx_aliases_active = [a for a in rx_aliases if a in cfg.rx_by_alias and a not in cfg.rx_skipped_aliases]
    if not rx_aliases_active:
        return [], {}
    failures: List[Tuple[str, str, str]] = []
    status_by_rx: Dict[str, Dict[str, str]] = {}
    if hasattr(amx, "send_command_with_status"):
        results = await asyncio.gather(
            *(
                amx.send_command_with_status(
                    decoder_ip=cfg.rx_by_alias[a].amx_decoder_ip,
                    command=command,
                    timeout_ms=timeout_ms,
                )
                for a in rx_aliases_active
            ),
            return_exceptions=True,
        )
        for a, res in zip(rx_aliases_active, results):
            if isinstance(res, BaseException):
                failures.append((a, cfg.rx_by_alias[a].amx_decoder_ip, str(res)))
                state.set_rx_online(a, False)
                state.set_rx_hdmi_output(a, None)
            else:
                state.set_rx_online(a, True)
                fields = res if isinstance(res, dict) else {}
                status_by_rx[a] = fields
                state.set_rx_hdmi_output(a, _hdmi_enabled_from_status_fields(fields))
        return failures, status_by_rx

    if hasattr(amx, "send_command"):
        results = await asyncio.gather(
            *(
                amx.send_command(
                    decoder_ip=cfg.rx_by_alias[a].amx_decoder_ip,
                    command=command,
                )
                for a in rx_aliases_active
            ),
            return_exceptions=True,
        )
        for a, res in zip(rx_aliases_active, results):
            if isinstance(res, BaseException):
                failures.append((a, cfg.rx_by_alias[a].amx_decoder_ip, str(res)))
                state.set_rx_online(a, False)
                state.set_rx_hdmi_output(a, None)
            else:
                state.set_rx_online(a, True)
    return failures, status_by_rx


def _handle_multiview_get(cfg: Config, cmd: str) -> List[str]:
    parts = cmd.split()
    if parts[:2] == ["mscene", "get"]:
        # Minimal empty layout list response.
        # If RX specified, return a single "mscene list:" + that RX with no layouts.
        if len(parts) == 3:
            rx = _lookup_rx(cfg, parts[2])
            if rx is None:
                return ["unknown command"]
            return ["mscene list:", f"{rx.alias}"]
        # all RX
        lines = ["mscene list:"]
        for rx in cfg.rx_by_alias.values():
            lines.append(f"{rx.alias}")
        return lines

    if parts[:2] == ["mview", "get"]:
        # Minimal empty custom layout response.
        if len(parts) == 3:
            rx = _lookup_rx(cfg, parts[2])
            if rx is None:
                return ["unknown command"]
            return ["mview information:", f"{rx.alias} tile"]
        lines = ["mview information:"]
        for rx in cfg.rx_by_alias.values():
            lines.append(f"{rx.alias} tile")
        return lines

    return ["unknown command"]


def _handle_videowall_get(cfg: Config, cmd: str) -> List[str]:
    parts = cmd.split()
    if parts[:2] == ["scene", "get"]:
        return ["scene list:"]
    if parts[:2] == ["vw", "get"]:
        return ["Video wall information:"]
    if parts[:2] == ["wscene2", "get"]:
        return ["wscene2 list:"]
    return ["unknown command"]


async def handle_client(
    cfg: Config,
    amx: Any,
    state: ControllerState,
    notifier: LocalProblemReporter,
    health: HealthState,
    runtime: RuntimeSettings,
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
) -> None:
    peer = writer.get_extra_info("peername")
    LOG.info("RTI connected from %s", peer)
    session = NhdCtlSession()
    health.rti_clients += 1

    def _write_rti_line(resp_line: str) -> None:
        if runtime.expanded_log:
            LOG.info("RTI <- %s", resp_line)
        writer.write(_crlf(resp_line))

    async def _read_protocol_line() -> Optional[str]:
        """
        Read one controller command line, accepting CRLF, LF, or CR delimiters.
        Some RTI driver flows may emit CR-only lines during reinitialize.
        Also strip/respond to Telnet negotiation bytes (IAC sequences).
        """
        IAC = 255
        DONT = 254
        DO = 253
        WONT = 252
        WILL = 251
        SB = 250
        SE = 240

        async def _telnet_filter_and_respond(data: bytes) -> bytes:
            out = bytearray()
            i = 0
            while i < len(data):
                b = data[i]
                if b != IAC:
                    out.append(b)
                    i += 1
                    continue

                # IAC at end of chunk: drop and continue.
                if i + 1 >= len(data):
                    break
                cmd = data[i + 1]

                # Escaped IAC (0xFF 0xFF) within text stream.
                if cmd == IAC:
                    out.append(IAC)
                    i += 2
                    continue

                # Subnegotiation: IAC SB ... IAC SE
                if cmd == SB:
                    j = i + 2
                    while j + 1 < len(data):
                        if data[j] == IAC and data[j + 1] == SE:
                            j += 2
                            break
                        j += 1
                    i = j
                    continue

                # Option negotiation: respond negatively to keep raw line protocol.
                if cmd in (DO, DONT, WILL, WONT):
                    if i + 2 < len(data):
                        opt = data[i + 2]
                        if cmd in (DO, DONT):
                            # Peer asks us to DO/DON'T -> we reply WONT.
                            writer.write(bytes([IAC, WONT, opt]))
                        else:
                            # Peer says WILL/WON'T -> we reply DONT.
                            writer.write(bytes([IAC, DONT, opt]))
                        with contextlib.suppress(Exception):
                            await writer.drain()
                        i += 3
                        continue
                    # Incomplete negotiation bytes at end of chunk.
                    break

                # Other 2-byte telnet command; ignore.
                i += 2

            return bytes(out)

        if not hasattr(_read_protocol_line, "_buf"):
            setattr(_read_protocol_line, "_buf", bytearray())
        buf: bytearray = getattr(_read_protocol_line, "_buf")
        while True:
            for i, b in enumerate(buf):
                if b in (10, 13):  # LF or CR
                    raw = bytes(buf[:i])
                    j = i + 1
                    # Collapse optional paired newline byte (CRLF / LFCR).
                    if j < len(buf) and buf[j] in (10, 13) and buf[j] != b:
                        j += 1
                    del buf[:j]
                    line = raw.decode("utf-8", errors="replace").strip()
                    if not line:
                        return ""
                    return line
            chunk = await reader.read(4096)
            if not chunk:
                if buf:
                    raw = bytes(buf)
                    buf.clear()
                    line = raw.decode("utf-8", errors="replace").strip()
                    return line or None
                return None
            app_bytes = await _telnet_filter_and_respond(chunk)
            if app_bytes:
                buf.extend(app_bytes)

    try:
        while True:
            line = await _read_protocol_line()
            if line is None:
                break
            if not line:
                continue

            LOG.info("RTI -> %s", line)
            lower = line.lower()
            parts = line.split()
            parts_lower = [p.lower() for p in parts]
            line_norm = " ".join(parts)

            # Handle known commands.
            if len(parts) >= 4 and parts_lower[:2] == ["matrix", "set"]:
                ok = False
                resp = "unknown command"
                failures: List[Tuple[str, str, str]] = []
                status_by_rx: Dict[str, Dict[str, str]] = {}
                try:
                    # For matrix set, mirror the raw incoming command exactly.
                    # old (normalized mirror): cfg, amx, state, line_norm, runtime.amx_verify_timeout_ms
                    ok, resp, failures, status_by_rx = await _handle_matrix_set(
                        cfg, amx, state, line, runtime.amx_verify_timeout_ms
                    )
                    if ok:
                        tx_token = parts[2]
                        tx_alias: Optional[str]
                        if tx_token.upper() == "NULL":
                            tx_alias = None
                        else:
                            tx_obj = _lookup_tx(cfg, tx_token)
                            tx_alias = tx_obj.alias if tx_obj is not None else None

                        rx_aliases: List[str] = []
                        for tok in parts[3:]:
                            rx_obj = _lookup_rx(cfg, tok)
                            if rx_obj is not None:
                                rx_aliases.append(rx_obj.alias)
                        state.set_all_media(tx_alias=tx_alias, rx_aliases=rx_aliases)
                        # Prefer AMX-reported STREAM for touched RXs; if missing/unusable, fall back to NULL.
                        if tx_alias is not None:
                            failed_rx = {rx_a for (rx_a, _ip, _err) in failures}
                            for rx_a in rx_aliases:
                                if rx_a in failed_rx:
                                    state.set_rx_all_media(rx_alias=rx_a, tx_alias=None)
                                    continue
                                stream_reported = (status_by_rx.get(rx_a, {}).get("STREAM") or "").strip()
                                # If AMX did not return status for this RX (e.g. skipped/non-polled),
                                # keep the requested route instead of forcing NULL.
                                if not stream_reported:
                                    state.set_rx_all_media(rx_alias=rx_a, tx_alias=tx_alias)
                                else:
                                    amx_tx_alias = _tx_alias_from_amx_stream(cfg, stream_reported)
                                    state.set_rx_all_media(
                                        rx_alias=rx_a,
                                        tx_alias=(amx_tx_alias if amx_tx_alias is not None else tx_alias),
                                    )

                        # Optional AMX verification (problems-only)
                        if (not cfg.amx_dry_run) and runtime.amx_verify_after_set and tx_alias is not None:
                            tx_obj2 = _lookup_tx(cfg, tx_alias)
                            expected = str(tx_obj2.amx_stream) if tx_obj2 is not None else None
                            if expected:
                                for rx_a in rx_aliases:
                                    ip = cfg.rx_by_alias[rx_a].amx_decoder_ip
                                    got = (status_by_rx.get(rx_a, {}).get("STREAM") or "").strip()
                                    if got != expected:
                                        await notifier.problem(
                                            f"amx.verify.{ip}",
                                            f"DT: ERROR AMX verify failed: {rx_a} expected STREAM {expected}",
                                        )
                except Exception as e:
                    LOG.exception("AMX routing failed")
                    await notifier.problem("amx.route", f"DT: ERROR AMX route failed: {e}")
                    # Keep RTI driver happy: still mirror the command as success.
                    ok, resp = True, line

                if failures:
                    for (rx_a, ip, err) in failures:
                        LOG.error("AMX SEND FAIL route decoder=%s rx=%s cmd=%r err=%s", ip, rx_a, f"set:{tx_obj.amx_stream}" if tx_obj is not None else "set:<unknown>", err)
                        state.set_rx_online(rx_a, False)
                        await notifier.problem(f"amx.set.{ip}", f"DT: ERROR AMX route failed: {rx_a} ({ip}): {err}")
                    await notifier.problem(
                        "amx.route.partial",
                        "DT: ERROR AMX route failed on: "
                        + ", ".join(f"{rx_a}({ip})" for (rx_a, ip, _e) in failures[:3])
                        + (" ..." if len(failures) > 3 else ""),
                    )
                else:
                    # Mark RX as online on successful send
                    try:
                        for tok in parts[3:]:
                            rx_obj = _lookup_rx(cfg, tok)
                            if rx_obj is not None:
                                state.set_rx_online(rx_obj.alias, True)
                    except Exception:
                        pass

                _write_rti_line(resp if ok else "unknown command")
                await writer.drain()
                continue

            # Breakaway switching
            if len(parts) >= 5 and parts_lower[0] == "matrix" and parts_lower[2] == "set":
                # matrix <kind> set <TX|NULL> <RX...>
                if len(parts) >= 5 and parts_lower[0] == "matrix" and parts_lower[2] == "set":
                    kind = parts[1].lower()
                    tx_token = parts[3]
                    rx_tokens = parts[4:]

                    tx_obj = _lookup_tx(cfg, tx_token)
                    tx_alias = None
                    if tx_obj is None:
                        if tx_token.upper() != "NULL":
                            _write_rti_line("unknown command")
                            await writer.drain()
                            continue
                    else:
                        tx_alias = tx_obj.alias

                    rx_aliases: List[str] = []
                    for tok in rx_tokens:
                        rx_obj = _lookup_rx(cfg, tok)
                        if rx_obj is None:
                            _write_rti_line("unknown command")
                            await writer.drain()
                            break
                        rx_aliases.append(rx_obj.alias)
                    else:
                        state.set_breakaway(kind=kind, tx_alias=tx_alias, rx_aliases=rx_aliases)

                        # Only video breakaway affects AMX in our model
                        if kind == "video" and tx_obj is not None:
                            try:
                                failures, status_by_rx = await _apply_amx_command_to_rx_aliases(
                                    cfg=cfg,
                                    amx=amx,
                                    state=state,
                                    rx_aliases=rx_aliases,
                                    command=f"set:{tx_obj.amx_stream}",
                                    timeout_ms=runtime.amx_verify_timeout_ms,
                                )
                                if failures:
                                    for (rx_a, ip, err) in failures:
                                        LOG.error("AMX SEND FAIL breakaway decoder=%s rx=%s cmd=%r err=%s", ip, rx_a, f"set:{tx_obj.amx_stream}", err)
                                        await notifier.problem(
                                            f"amx.set.{ip}",
                                            f"DT: ERROR AMX breakaway video route failed: {rx_a} ({ip}): {err}",
                                        )
                                    await notifier.problem(
                                        "amx.breakaway.video.partial",
                                        "DT: ERROR AMX breakaway video route failed on: "
                                        + ", ".join(f"{rx_a}({ip})" for (rx_a, ip, _e) in failures[:3])
                                        + (" ..." if len(failures) > 3 else ""),
                                    )

                                # Prefer AMX-reported STREAM for touched RXs; if missing/unusable, fall back to NULL.
                                failed_rx = {rx_a for (rx_a, _ip, _err) in failures}
                                for rx_a in rx_aliases:
                                    if rx_a in failed_rx:
                                        state.set_breakaway(kind="video", tx_alias=None, rx_aliases=[rx_a])
                                        continue
                                    stream_reported = (status_by_rx.get(rx_a, {}).get("STREAM") or "").strip()
                                    if not stream_reported:
                                        state.set_breakaway(kind="video", tx_alias=tx_alias, rx_aliases=[rx_a])
                                    else:
                                        amx_tx_alias = _tx_alias_from_amx_stream(cfg, stream_reported)
                                        state.set_breakaway(
                                            kind="video",
                                            tx_alias=(amx_tx_alias if amx_tx_alias is not None else tx_alias),
                                            rx_aliases=[rx_a],
                                        )

                                if (not cfg.amx_dry_run) and runtime.amx_verify_after_set:
                                    expected = str(tx_obj.amx_stream)
                                    for rx_a in rx_aliases:
                                        got = (status_by_rx.get(rx_a, {}).get("STREAM") or "").strip()
                                        ip = cfg.rx_by_alias[rx_a].amx_decoder_ip
                                        if got != expected:
                                            await notifier.problem(
                                                f"amx.verify.{ip}",
                                                f"DT: ERROR AMX verify failed: {rx_a} expected STREAM {expected}",
                                            )
                            except Exception as e:
                                LOG.exception("AMX routing failed")
                                await notifier.problem(
                                    "amx.breakaway.video", f"DT: ERROR AMX breakaway video route failed: {e}"
                                )

                        # For matrix <kind> set, mirror the raw incoming command exactly.
                        # old (normalized mirror): _write_rti_line(line_norm)
                        _write_rti_line(line)  # command mirror ack
                        await writer.drain()
                        continue

            # Matrix query commands used for RTI feedback variables
            if len(parts) >= 2 and parts_lower[0] == "matrix" and "get" in parts_lower:
                # matrix get [<RX...>]  — primary all-media assignments (§13.3)
                if len(parts) >= 2 and parts_lower[0] == "matrix" and parts_lower[1] == "get":
                    rx_tokens = parts[2:]
                    rx_aliases_m: List[str] = []
                    if rx_tokens:
                        for tok in rx_tokens:
                            rx_obj = _lookup_rx(cfg, tok)
                            if rx_obj is None:
                                _write_rti_line("unknown command")
                                await writer.drain()
                                break
                            rx_aliases_m.append(rx_obj.alias)
                        else:
                            pass
                    else:
                        rx_aliases_m = list(cfg.rx_by_alias.keys())
                    if len(rx_tokens) and len(rx_aliases_m) != len(rx_tokens):
                        continue
                    matrix_lines = _format_matrix_info(
                        heading="matrix", mapping=state.video, rx_aliases=rx_aliases_m
                    )
                    for resp_line in matrix_lines:
                        _write_rti_line(resp_line)
                        await writer.drain()
                    # Match observed WyreStorm framing: terminate matrix blocks with blank lines.
                    _write_rti_line("")
                    await writer.drain()
                    _write_rti_line("")
                    await writer.drain()
                    continue
                # Examples:
                # matrix video get [<RX...>]
                # matrix audio get [<RX...>]
                if len(parts) >= 3 and parts_lower[0] == "matrix" and parts_lower[2] == "get":
                    kind = parts[1].lower()
                    rx_tokens = parts[3:]
                    rx_aliases: List[str] = []
                    if rx_tokens:
                        for tok in rx_tokens:
                            rx_obj = _lookup_rx(cfg, tok)
                            if rx_obj is None:
                                _write_rti_line("unknown command")
                                await writer.drain()
                                break
                            rx_aliases.append(rx_obj.alias)
                        else:
                            # all rx parsed ok
                            pass
                    else:
                        rx_aliases = list(cfg.rx_by_alias.keys())

                    table = {
                        "video": state.video,
                        "audio": state.audio,
                        "audio2": state.audio,
                        "usb": state.usb,
                        "serial": state.serial,
                        "infrared": state.infrared,
                    }.get(kind)
                    if table is None:
                        _write_rti_line("unknown command")
                        await writer.drain()
                        continue

                    matrix_lines = _format_matrix_info(
                        heading=f"matrix {kind}", mapping=table, rx_aliases=rx_aliases
                    )
                    for resp_line in matrix_lines:
                        _write_rti_line(resp_line)
                        await writer.drain()
                    # Match observed WyreStorm framing: terminate matrix blocks with blank lines.
                    _write_rti_line("")
                    await writer.drain()
                    _write_rti_line("")
                    await writer.drain()
                    continue

            if lower.startswith("config set session alias "):
                parts = line.split()
                if len(parts) == 5 and parts[4].lower() in ("on", "off"):
                    session.alias_mode = parts[4].lower() == "on"
                    _write_rti_line(line_norm)  # command mirror ack
                else:
                    _unknown_ctl_record(line)
                    _write_rti_line("unknown command")
                await writer.drain()
                continue

            if (
                len(parts) >= 6
                and parts_lower[0] == "config"
                and parts_lower[1] == "set"
                and parts_lower[2] == "device"
                and parts_lower[3] == "cec"
            ):
                cec_mode = parts_lower[4]
                hdmi_enabled: Optional[bool] = None
                if cec_mode in ("onetouchplay", "on"):
                    hdmi_enabled = True
                elif cec_mode in ("standby", "off"):
                    hdmi_enabled = False
                else:
                    _unknown_ctl_record(line)
                    _write_rti_line("unknown command")
                    await writer.drain()
                    continue

                rx_aliases: List[str] = []
                for tok in parts[5:]:
                    rx_obj = _lookup_rx(cfg, tok)
                    if rx_obj is None:
                        _write_rti_line("unknown command")
                        await writer.drain()
                        break
                    rx_aliases.append(rx_obj.alias)
                else:
                    LOG.info(
                        "CEC translate: %s -> %s for %d RX(s): %s",
                        cec_mode,
                        "hdmiOn" if hdmi_enabled else "hdmiOff",
                        len(rx_aliases),
                        ", ".join(rx_aliases),
                    )
                    try:
                        failures, _status_by_rx = await _apply_amx_command_to_rx_aliases(
                            cfg=cfg,
                            amx=amx,
                            state=state,
                            rx_aliases=rx_aliases,
                            command="hdmiOn" if hdmi_enabled else "hdmiOff",
                            timeout_ms=runtime.amx_verify_timeout_ms,
                        )
                    except Exception as e:
                        LOG.exception("AMX HDMI control failed")
                        await notifier.problem("amx.hdmi", f"DT: ERROR AMX HDMI control failed: {e}")
                        failures = []

                    if failures:
                        for (rx_a, ip, err) in failures:
                            LOG.error("AMX SEND FAIL cec decoder=%s rx=%s cmd=%r err=%s", ip, rx_a, "hdmiOn" if hdmi_enabled else "hdmiOff", err)
                            await notifier.problem(
                                f"amx.hdmi.{ip}",
                                f"DT: ERROR AMX HDMI control failed: {rx_a} ({ip}): {err}",
                            )
                        await notifier.problem(
                            "amx.hdmi.partial",
                            "DT: ERROR AMX HDMI control failed on: "
                            + ", ".join(f"{rx_a}({ip})" for (rx_a, ip, _e) in failures[:3])
                            + (" ..." if len(failures) > 3 else ""),
                        )
                    _write_rti_line(line_norm)
                    await writer.drain()
                    continue

            if lower.startswith("config set "):
                # For many config set commands, WyreStorm replies with command mirror.
                _write_rti_line(line_norm)
                await writer.drain()
                continue

            if lower.startswith("config get "):
                _cg_out = _handle_config_get(cfg, session, state, line)
                if _cg_out == ["unknown command"]:
                    _unknown_ctl_record(line)
                for resp_line in _cg_out:
                    _write_rti_line(resp_line)
                    await writer.drain()
                # Match observed WyreStorm framing for multi-line config get responses.
                if len(_cg_out) > 1 and _cg_out != ["unknown command"]:
                    _write_rti_line("")
                    await writer.drain()
                    _write_rti_line("")
                await writer.drain()
                continue

            # Safe mirrors / minimal responses for RTI driver feature surface.
            # Video wall + multiview query commands (return empty lists rather than "unknown command")
            if lower.startswith(("scene get", "vw get", "wscene2 get")):
                _vw_out = _handle_videowall_get(cfg, line)
                if _vw_out == ["unknown command"]:
                    _unknown_ctl_record(line)
                for resp_line in _vw_out:
                    _write_rti_line(resp_line)
                await writer.drain()
                continue

            if lower.startswith(("mscene get", "mview get")):
                _mv_out = _handle_multiview_get(cfg, line)
                if _mv_out == ["unknown command"]:
                    _unknown_ctl_record(line)
                for resp_line in _mv_out:
                    _write_rti_line(resp_line)
                await writer.drain()
                continue

            # Scene/multiview activation and edits: acknowledge success.
            if lower.startswith(
                (
                    "scene active ",
                    "wscene2 active ",
                    "vw active ",
                    "mscene active ",
                    "mscene change ",
                    "mscene set ",
                    "mview set ",
                    "mview set audio ",
                    "cec ",
                    "infrared ",
                    "serial ",
                    "api ",
                )
            ):
                # Some of these commands have defined response structure with success|failure.
                if lower.startswith(("mscene active ", "mscene change ", "mscene set ", "mview set ", "mview set audio ")):
                    _write_rti_line(_as_success(line_norm))
                else:
                    _write_rti_line(line_norm)
                await writer.drain()
                continue

            # Unknown command (no handler matched)
            _unknown_ctl_record(line)
            _write_rti_line("unknown command")
            await writer.drain()

    finally:
        LOG.info("RTI disconnected from %s", peer)
        health.rti_clients = max(0, health.rti_clients - 1)
        writer.close()
        with contextlib.suppress(Exception):
            await writer.wait_closed()


async def run_server(*, cfg: Config, config_path: str, listen: str, port: int) -> None:
    started_at = time.monotonic()
    _cfg_dir = Path(config_path).expanduser().resolve().parent
    _unknown_ctl_configure(
        enabled=cfg.unknown_ctl_enabled,
        config_dir=_cfg_dir,
        persist_path=cfg.unknown_ctl_persist_path,
    )
    _unknown_ctl_load_from_disk()
    # Optional RTI UDP control listener (e.g., reboot command)
    if cfg.rti_control_enabled and cfg.rti_control_port > 0:
        loop = asyncio.get_running_loop()
        bind = cfg.rti_control_bind_address or "0.0.0.0"
        await loop.create_datagram_endpoint(
            lambda: _RtiControlUdp(cfg=cfg),
            local_addr=(bind, cfg.rti_control_port),
        )
        LOG.warning("RTI control UDP listening on %s:%d", bind, cfg.rti_control_port)
    if cfg.amx_bind_address:
        LOG.info("AMX outbound connections will bind to %s (AVoIP NIC)", cfg.amx_bind_address)

    if cfg.amx_dry_run:
        LOG.warning("AMX dry-run enabled: no TCP connections will be made.")
        if cfg.amx_dry_run_offline_decoders:
            LOG.warning(
                "AMX dry-run offline simulation decoders: %s",
                ", ".join(cfg.amx_dry_run_offline_decoders),
            )
        amx: Any = DryRunAmxClient(
            decoder_port=cfg.amx_decoder_port,
            offline_decoders=cfg.amx_dry_run_offline_decoders,
        )
    elif cfg.amx_persistent:
        LOG.warning("AMX persistent mode enabled: keeping per-decoder sockets open.")
        amx = PersistentAmxClient(
            decoder_port=cfg.amx_decoder_port,
            connect_timeout_ms=cfg.amx_connect_timeout_ms,
            command_timeout_ms=cfg.amx_command_timeout_ms,
            keepalive_seconds=cfg.amx_keepalive_seconds,
            bind_address=cfg.amx_bind_address,
            set_queue_limit=cfg.amx_set_queue_limit,
            set_retry_attempts=cfg.amx_set_retry_attempts,
            set_retry_backoff_initial_ms=cfg.amx_set_retry_backoff_initial_ms,
            set_retry_backoff_max_ms=cfg.amx_set_retry_backoff_max_ms,
            expanded_log=cfg.expanded_log,
        )
    else:
        amx = AmxClient(
            decoder_port=cfg.amx_decoder_port,
            connect_timeout_ms=cfg.amx_connect_timeout_ms,
            command_timeout_ms=cfg.amx_command_timeout_ms,
            bind_address=cfg.amx_bind_address,
            set_retry_attempts=cfg.amx_set_retry_attempts,
            set_retry_backoff_initial_ms=cfg.amx_set_retry_backoff_initial_ms,
            set_retry_backoff_max_ms=cfg.amx_set_retry_backoff_max_ms,
            expanded_log=cfg.expanded_log,
        )

    state = ControllerState(cfg)
    if cfg.amx_dry_run:
        # Dry-run baseline: emulate RX status as STREAM:1 and HDMI enabled.
        tx_stream1_alias: Optional[str] = None
        for tx in cfg.tx_by_alias.values():
            if int(tx.amx_stream) == 1:
                tx_stream1_alias = tx.alias
                break
        if tx_stream1_alias is not None:
            for rx in cfg.rx_by_alias.values():
                if rx.alias in cfg.rx_skipped_aliases:
                    continue
                state.set_rx_all_media(rx_alias=rx.alias, tx_alias=tx_stream1_alias)
                state.set_rx_hdmi_output(rx.alias, True)
            LOG.info(
                "Dry-run startup seed: set all RX routes to %s (amx_stream=1), HDMI output ON",
                tx_stream1_alias,
            )
        else:
            LOG.warning("Dry-run startup seed skipped: no TX with amx_stream=1 in config")
    if cfg.amx_dry_run and cfg.amx_dry_run_offline_decoders:
        offline = {x.strip() for x in cfg.amx_dry_run_offline_decoders if str(x).strip()}
        for rx in cfg.rx_by_alias.values():
            if rx.alias in cfg.rx_skipped_aliases:
                continue
            if rx.amx_decoder_ip in offline:
                state.set_rx_online(rx.alias, False)
    elif not cfg.amx_dry_run:
        # In live mode, avoid optimistic "connected" until we have evidence.
        for rx in cfg.rx_by_alias.values():
            if rx.alias in cfg.rx_skipped_aliases:
                continue
            state.set_rx_online(rx.alias, False)
    health = HealthState()
    runtime = RuntimeSettings(cfg)
    problems = ProblemState()

    notifier = LocalProblemReporter(
        min_interval_seconds=10,
        repeat_suppression_seconds=300,
    )
    notifier.attach_problem_state(problems)

    async def _run_startup_self_test() -> None:
        # Run in background so web/RTI listeners come up immediately.
        try:
            res = await _amx_self_test(cfg=cfg, amx=amx)
            unreachable = {str(x).strip() for x in (res.get("unreachable") or [])}
            # Reflect startup connectivity on the status page.
            for rx in cfg.rx_by_alias.values():
                if rx.alias in cfg.rx_skipped_aliases:
                    continue
                state.set_rx_online(rx.alias, rx.amx_decoder_ip not in unreachable)
            fail = res.get("unreachable") or []
            if fail:
                fail_l = list(fail)
                await notifier.problem(
                    "amx.selftest",
                    f"DT: ERROR AMX self-test: {res.get('ok', 0)}/{res.get('total', 0)} reachable. Unreachable: {', '.join(fail_l[:5])}"
                    + (" ..." if len(fail_l) > 5 else ""),
                )
        except Exception:
            LOG.exception("Startup AMX self-test failed")
        try:
            # Also prime TX status on startup so the status page has immediate TX visibility.
            await _refresh_tx_statuses(cfg=cfg, state=state, runtime=runtime)
            offline_txs = sorted(
                [
                    tx_alias
                    for tx_alias in cfg.tx_by_alias.keys()
                    if tx_alias not in cfg.tx_skipped_aliases and not state.tx_online.get(tx_alias, False)
                ],
                key=_tx_alias_sort_key,
            )
            if offline_txs:
                await notifier.problem(
                    "amx.txstatus.startup",
                    f"DT: ERROR AMX TX startup status poll: {len(offline_txs)}/{max(1, len(cfg.tx_by_alias) - len(cfg.tx_skipped_aliases))} offline. "
                    + ", ".join(offline_txs[:5])
                    + (" ..." if len(offline_txs) > 5 else ""),
                )
        except Exception:
            LOG.exception("Startup AMX TX status poll failed")

    tx_poller = TxStatusPoller(cfg=cfg, state=state, runtime=runtime)
    await tx_poller.start()

    if cfg.http_status_enabled:
        http_server = await asyncio.start_server(
            lambda r, w: _handle_http_client(
                r,
                w,
                cfg=cfg,
                health=health,
                amx=amx,
                state=state,
                runtime=runtime,
                problems=problems,
                started_at=started_at,
                config_path=config_path,
            ),
            host=cfg.http_status_bind,
            port=cfg.http_status_port,
        )
        addrs = ", ".join(str(sock.getsockname()) for sock in (http_server.sockets or []))
        LOG.info("HTTP status listening on %s", addrs)

    server = await asyncio.start_server(
        lambda r, w: handle_client(cfg, amx, state, notifier, health, runtime, r, w),
        host=listen,
        port=port,
    )

    addrs = ", ".join(str(sock.getsockname()) for sock in (server.sockets or []))
    LOG.info("Listening on %s", addrs)

    # Optional AMX self-test on startup (problems-only notification), non-blocking.
    if cfg.amx_self_test_on_start:
        asyncio.create_task(_run_startup_self_test())

    async with server:
        await server.serve_forever()


# ---------------------------------------------------------------------------
# Process entrypoint
# ---------------------------------------------------------------------------
def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="RTI -> WyreStorm NHD-CTL emulator -> AMX AVoIP translator")
    parser.add_argument("--config", required=True, help="Path to config.json")
    parser.add_argument("--listen", default="0.0.0.0", help="Address to bind (default 0.0.0.0)")
    parser.add_argument("--port", type=int, default=2323, help="TCP port to listen on (default 2323)")
    parser.add_argument("--log-level", default="INFO", help="DEBUG, INFO, WARNING, ERROR")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    ring = _RingBufferLogHandler()
    ring.setLevel(logging.DEBUG)
    ring.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
    logging.getLogger().addHandler(ring)

    cfg = load_config(args.config)
    _validate_config(cfg)

    try:
        asyncio.run(
            run_server(cfg=cfg, config_path=args.config, listen=args.listen, port=args.port)
        )
    except KeyboardInterrupt:
        return 130
    return 0

