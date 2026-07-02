from __future__ import annotations

import asyncio
import contextlib
import html
import json
import logging
from pathlib import Path
import urllib.parse
from typing import Any, Awaitable, Callable, Dict, List

from .av_scan import (
    check_av_bind_address,
    infer_default_scan_range,
    next_suggested_aliases,
    request_av_scan_cancel,
    scan_av_network,
)
from .config_persistence import (
    append_av_endpoints_to_config,
    load_endpoint_inventory,
    persist_endpoint_skip_to_config,
    persist_endpoints_to_config,
    persist_runtime_setting_to_config,
)
from .http_helpers import (
    build_status_snapshot,
    control_feedback_html,
    format_uptime,
    http_response,
    http_unauthorized,
    params_want_html,
    parse_basic_auth_password,
)
from .http_ui_session import (
    issue_session_token as http_ui_sess_issue,
    parse_path_params as http_parse_path_params,
    valid_session_token as http_ui_sess_valid,
)
from .log_ring import get_log_tail
from .matrix_amx import process_matrix_set_line
from .models import (
    Config,
    ControllerState,
    HealthState,
    ProblemState,
    RuntimeSettings,
)
from .problem_reporter import LocalProblemReporter
from .rti_status import RtiStatusReporter
from . import staff_notes
from .protocol_helpers import format_tx_signal
from .system_control import do_reboot, do_service_restart
from .unknown_ctl import (
    clear_persisted as unknown_ctl_clear_persisted,
    page_text as unknown_ctl_page_text,
    persist_file as unknown_ctl_persist_file,
)
from .constants import RX_STATUS_POLL_INTERVAL_SECONDS, TX_STATUS_POLL_INTERVAL_SECONDS
from .utils import as_bool, rx_alias_sort_key, tx_alias_sort_key

LOG = logging.getLogger("drivertranslator")

_HTTP_STATUS_LINES = {
    200: "200 OK",
    400: "400 Bad Request",
    405: "405 Method Not Allowed",
}

_ASSET_INTEGRION_LOGO_PATH = Path(__file__).resolve().parents[1] / "Integrion_logo.png"
_ASSET_CONDUCTOR1_LOGO_PATH = Path(__file__).resolve().parents[1] / "Conductor1.png"


async def _read_http_body(reader: asyncio.StreamReader, data: bytes) -> bytes:
    _, _, body = data.partition(b"\r\n\r\n")
    content_length = 0
    for line in data.split(b"\r\n")[1:]:
        if line.lower().startswith(b"content-length:"):
            try:
                content_length = int(line.split(b":", 1)[1].strip())
            except ValueError:
                pass
            break
    if content_length > len(body):
        body += await reader.read(content_length - len(body))
    return body[:content_length] if content_length else body


async def handle_http_client(
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
    amx_self_test: Callable[..., Awaitable[Dict[str, Any]]],
    notifier: LocalProblemReporter,
    status_reporter: Optional[RtiStatusReporter] = None,
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
            writer.write(http_response("400 Bad Request", "text/plain", b"bad request"))
            return
        method, path = parts[0], parts[1]
        path_only, early_params = http_parse_path_params(path)
        control_via_ui = path_only.startswith("/control/") and http_ui_sess_valid(
            early_params.get("ui_sess", "")
        )
        notes_public = path_only == "/anteckningar" or path_only == "/anteckningar/api"

        if path_only == "/assets/integrion_logo.png":
            try:
                body = _ASSET_INTEGRION_LOGO_PATH.read_bytes()
            except FileNotFoundError:
                writer.write(http_response("404 Not Found", "text/plain", b"not found"))
                return
            writer.write(http_response("200 OK", "image/png", body))
            return

        if path_only == "/assets/conductor1.png":
            try:
                body = _ASSET_CONDUCTOR1_LOGO_PATH.read_bytes()
            except FileNotFoundError:
                writer.write(http_response("404 Not Found", "text/plain", b"not found"))
                return
            writer.write(http_response("200 OK", "image/png", body))
            return

        # Require Basic auth (except /control/* with valid ui_sess, and staff notes).
        if cfg.http_status_password:
            pw = parse_basic_auth_password(data)
            if pw != cfg.http_status_password and not control_via_ui and not notes_public:
                writer.write(http_unauthorized())
                return

        if path_only == "/anteckningar" and method == "GET":
            writer.write(http_response("200 OK", "text/html; charset=utf-8", staff_notes.page_html()))
            return

        if path_only == "/anteckningar/api":
            body = b""
            if method == "POST":
                body = await _read_http_body(reader, data)
            code, content_type, resp_body = staff_notes.handle_api(method, body)
            writer.write(http_response(_HTTP_STATUS_LINES.get(code, f"{code}"), content_type, resp_body))
            return

        if method != "GET":
            writer.write(http_response("405 Method Not Allowed", "text/plain", b"method not allowed"))
            return

        snapshot = build_status_snapshot(cfg=cfg, health=health, amx=amx, started_at=started_at)
        rt = await runtime.snapshot()
        _ = await problems.snapshot()

        # JSON snapshot API — /status is reserved for the Status HTML tab (see below).
        if path_only == "/status.json":
            body = (json.dumps(snapshot, indent=2) + "\n").encode("utf-8")
            writer.write(http_response("200 OK", "application/json", body))
            return

        if path in ("/logs", "/logs.json"):
            body = (json.dumps({"lines": get_log_tail(rt["http_log_lines"])}, indent=2) + "\n").encode(
                "utf-8"
            )
            writer.write(http_response("200 OK", "application/json", body))
            return

        if path in ("/control", "/control.json"):
            body = (json.dumps(rt, indent=2) + "\n").encode("utf-8")
            writer.write(http_response("200 OK", "application/json", body))
            return

        if path in ("/problems", "/problems.json"):
            prob = await problems.snapshot()
            body = (json.dumps({"problems": prob}, indent=2) + "\n").encode("utf-8")
            writer.write(http_response("200 OK", "application/json", body))
            return

        # Basic control endpoints (optional token).
        # /control/set?key=<k>&value=<v>[&token=<t>]
        # /control/set_endpoints?tx_count=<n>&rx_count=<n>&tx_start_ip=<ip>&rx_start_ip=<ip>
        # /control/set_endpoint_skip?kind=tx|rx&alias=<alias>&skip=true|false
        # /control/selftest?[token=<t>]
        # /control/restart
        # /control/reboot
        # /control/clear_unknown_ctl
        # /control/av_scan_abort[&token=]
        # /control/av_scan?range=<cidr|start-end>[&token=]
        # /control/add_av_endpoints?tx_ips=a,b&rx_ips=c,d[&token=]
        # /control/matrix_set?tx=<TX|NULL>&rx=<RX>
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
                if params_want_html(params):
                    writer.write(
                        http_response(
                            "403 Forbidden",
                            "text/html; charset=utf-8",
                            control_feedback_html(
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
                    writer.write(http_response("403 Forbidden", "text/plain", b"forbidden"))
                return

            ctl_via = "status_page" if params.get("ui_sess") else "http_api"

            if path.startswith("/control/set_endpoints"):
                want_html = params_want_html(params)
                tx_count_s = params.get("tx_count", "").strip()
                rx_count_s = params.get("rx_count", "").strip()
                tx_start_ip = params.get("tx_start_ip", "").strip()
                rx_start_ip = params.get("rx_start_ip", "").strip()
                try:
                    if not tx_count_s or not rx_count_s or not tx_start_ip or not rx_start_ip:
                        raise ValueError("All parameters are required.")
                    res = persist_endpoints_to_config(
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
                            http_response(
                                "400 Bad Request",
                                "text/html; charset=utf-8",
                                control_feedback_html(
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
                        writer.write(http_response("400 Bad Request", "application/json", body))
                    return
                if want_html:
                    writer.write(
                        http_response(
                            "200 OK",
                            "text/html; charset=utf-8",
                            control_feedback_html(
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
                    writer.write(http_response("200 OK", "application/json", body))
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
                want_html = params_want_html(params)
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
                    res = persist_endpoint_skip_to_config(
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
                            http_response(
                                "400 Bad Request",
                                "text/html; charset=utf-8",
                                control_feedback_html(
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
                        writer.write(http_response("400 Bad Request", "application/json", body))
                    return
                if want_html:
                    writer.write(
                        http_response(
                            "200 OK",
                            "text/html; charset=utf-8",
                            control_feedback_html(
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
                    writer.write(http_response("200 OK", "application/json", body))
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
                want_html = params_want_html(params)
                try:
                    if value.lower() in ("true", "1", "yes", "y", "on", "false", "0", "no", "n", "off"):
                        await runtime.set_bool(key, value.lower() in ("true", "1", "yes", "y", "on"))
                    else:
                        await runtime.set_int(key, int(value))
                    snap = await runtime.snapshot()
                    persist_runtime_setting_to_config(
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
                        "Expected key amx_dry_run, amx_verify_after_set, amx_tx_poll_enabled, amx_rx_poll_enabled, rti_status_enabled, or expanded_log with true/false, "
                        "or amx_verify_timeout_ms with a number (100-5000)."
                    )
                    if want_html:
                        writer.write(
                            http_response(
                                "400 Bad Request",
                                "text/html; charset=utf-8",
                                control_feedback_html(
                                    ok=False,
                                    headline="Could not apply setting",
                                    paragraphs=[bad_msg],
                                ),
                            )
                        )
                    else:
                        writer.write(http_response("400 Bad Request", "text/plain", b"bad control request"))
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
                        paras = [
                            "amx_persistent is disabled in this build; non-persistent mode is always used.",
                            "Saved value is forced to false.",
                        ]
                    elif key == "amx_verify_after_set":
                        v = snap.get("amx_verify_after_set")
                        paras = [
                            f"amx_verify_after_set is now {str(v).lower()} (post-route AMX STREAM check; inactive while AMX RX polling is on).",
                            "Takes effect immediately; no service restart needed.",
                        ]
                    elif key == "expanded_log":
                        v = snap.get("expanded_log")
                        paras = [
                            f"expanded_log is now {str(v).lower()} (logs RTI responses and AMX replies).",
                            "Takes effect immediately; no service restart needed.",
                        ]
                    elif key == "amx_rx_poll_enabled":
                        v = snap.get("amx_rx_poll_enabled")
                        paras = [
                            f"amx_rx_poll_enabled is now {str(v).lower()} (polls RX getStatus every {RX_STATUS_POLL_INTERVAL_SECONDS}s).",
                            "Takes effect immediately; no service restart needed.",
                        ]
                    elif key == "amx_tx_poll_enabled":
                        v = snap.get("amx_tx_poll_enabled")
                        paras = [
                            f"amx_tx_poll_enabled is now {str(v).lower()} (polls TX getStatus every {TX_STATUS_POLL_INTERVAL_SECONDS}s).",
                            "Takes effect immediately; no service restart needed.",
                        ]
                    elif key == "rti_status_enabled":
                        v = snap.get("rti_status_enabled")
                        paras = [
                            f"rti_status_enabled is now {str(v).lower()} (TCP: per-TX DTTX booleans + one DTRXSUMMARY string for RX).",
                            "Takes effect immediately; no service restart needed.",
                        ]
                    else:
                        paras = [
                            f"Updated setting {key!r}.",
                            "Takes effect immediately; no service restart needed.",
                        ]
                    writer.write(
                        http_response(
                            "200 OK",
                            "text/html; charset=utf-8",
                            control_feedback_html(
                                ok=True,
                                headline="Saved",
                                paragraphs=paras,
                                pre_json={
                                    "amx_dry_run": snap.get("amx_dry_run"),
                                    "amx_persistent": snap.get("amx_persistent"),
                                    "amx_verify_after_set": snap.get("amx_verify_after_set"),
                                    "amx_tx_poll_enabled": snap.get("amx_tx_poll_enabled"),
                                    "amx_rx_poll_enabled": snap.get("amx_rx_poll_enabled"),
                                    "rti_status_enabled": snap.get("rti_status_enabled"),
                                    "amx_verify_timeout_ms": snap.get("amx_verify_timeout_ms"),
                                    "expanded_log": snap.get("expanded_log"),
                                },
                            ),
                        )
                    )
                else:
                    body = (json.dumps(snap, indent=2) + "\n").encode("utf-8")
                    writer.write(http_response("200 OK", "application/json", body))
                LOG.info(
                    "HTTP control [source=%s]: set %s=%r (dry_run=%s persistent=%s verify_after_set=%s tx_poll_enabled=%s rx_poll_enabled=%s verify_timeout_ms=%s expanded_log=%s)",
                    ctl_via,
                    key,
                    snap.get(key),
                    snap.get("amx_dry_run"),
                    snap.get("amx_persistent"),
                    snap.get("amx_verify_after_set"),
                    snap.get("amx_tx_poll_enabled"),
                    snap.get("amx_rx_poll_enabled"),
                    snap.get("amx_verify_timeout_ms"),
                    snap.get("expanded_log"),
                )
                return

            if path.startswith("/control/selftest"):
                res = await amx_self_test(cfg=cfg, amx=amx)
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
                if params_want_html(params):
                    if total == 0:
                        paras = ["No RX / decoders are configured in this profile."]
                    else:
                        paras = [f"TCP connect to port {cfg.amx_decoder_port}: {ok_n} of {total} reachable."]
                        if unr:
                            paras.append("Unreachable: " + ", ".join(str(x) for x in unr))
                        else:
                            paras.append("All configured decoder IPs accepted a connection.")
                    writer.write(
                        http_response(
                            "200 OK",
                            "text/html; charset=utf-8",
                            control_feedback_html(
                                ok=all_ok,
                                headline="Self-test passed" if all_ok else "Self-test: issues found",
                                paragraphs=paras,
                                pre_json=res,
                            ),
                        )
                    )
                else:
                    body = (json.dumps(res, indent=2) + "\n").encode("utf-8")
                    writer.write(http_response("200 OK", "application/json", body))
                return

            if path.startswith("/control/add_av_endpoints"):
                tx_raw = urllib.parse.unquote_plus(params.get("tx_ips", "").strip())
                rx_raw = urllib.parse.unquote_plus(params.get("rx_ips", "").strip())
                tx_ips = [x.strip() for x in tx_raw.split(",") if x.strip()]
                rx_ips = [x.strip() for x in rx_raw.split(",") if x.strip()]
                try:
                    res = append_av_endpoints_to_config(
                        config_path=config_path,
                        tx_ips=tx_ips,
                        rx_ips=rx_ips,
                    )
                except Exception as e:
                    LOG.warning(
                        "HTTP control [source=%s]: add_av_endpoints failed tx_ips=%r rx_ips=%r err=%s",
                        ctl_via,
                        tx_ips,
                        rx_ips,
                        e,
                    )
                    body = (json.dumps({"ok": False, "error": str(e)}, indent=2) + "\n").encode("utf-8")
                    writer.write(http_response("400 Bad Request", "application/json", body))
                    return
                LOG.info(
                    "HTTP control [source=%s]: add_av_endpoints added_tx=%s added_rx=%s skipped=%s",
                    ctl_via,
                    res.get("added_tx"),
                    res.get("added_rx"),
                    res.get("skipped"),
                )
                body = (json.dumps(res, indent=2) + "\n").encode("utf-8")
                writer.write(http_response("200 OK", "application/json", body))
                return

            if path.startswith("/control/av_scan_abort"):
                request_av_scan_cancel()
                LOG.info("HTTP control [source=%s]: av_scan_abort requested", ctl_via)
                body = (json.dumps({"ok": True, "cancel_requested": True}, indent=2) + "\n").encode("utf-8")
                writer.write(http_response("200 OK", "application/json", body))
                return

            if path.startswith("/control/av_scan"):
                range_spec = urllib.parse.unquote_plus(params.get("range", "").strip())
                if not range_spec:
                    range_spec = infer_default_scan_range(cfg)
                res = await scan_av_network(cfg=cfg, runtime=runtime, range_spec=range_spec)
                if res.get("ok"):
                    res["suggested_aliases"] = next_suggested_aliases(cfg)
                    res["default_range_hint"] = infer_default_scan_range(cfg)
                LOG.info(
                    "HTTP control [source=%s]: av_scan range=%r ok=%s found=%s cancelled=%s",
                    ctl_via,
                    range_spec,
                    res.get("ok"),
                    len(res.get("found") or []) if isinstance(res.get("found"), list) else None,
                    res.get("cancelled"),
                )
                if not res.get("ok"):
                    body = (json.dumps(res, indent=2) + "\n").encode("utf-8")
                    writer.write(http_response("400 Bad Request", "application/json", body))
                    return
                body = (json.dumps(res, indent=2) + "\n").encode("utf-8")
                writer.write(http_response("200 OK", "application/json", body))
                return

            if path.startswith("/control/clear_unknown_ctl"):
                unknown_ctl_clear_persisted()
                LOG.info("HTTP control [source=%s]: cleared unknown_ctl list", ctl_via)
                if params_want_html(params):
                    writer.write(
                        http_response(
                            "200 OK",
                            "text/html; charset=utf-8",
                            control_feedback_html(
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
                    writer.write(http_response("200 OK", "application/json", body))
                return

            if path.startswith("/control/matrix_set"):
                tx = urllib.parse.unquote_plus(params.get("tx", "").strip())
                rx = urllib.parse.unquote_plus(params.get("rx", "").strip())
                if not rx:
                    body = (json.dumps({"ok": False, "error": "missing rx parameter"}) + "\n").encode("utf-8")
                    writer.write(http_response("400 Bad Request", "application/json", body))
                    return
                if tx.upper() == "NULL" or tx == "":
                    cmd_line = f"matrix set NULL {rx}"
                else:
                    if tx not in cfg.tx_by_alias:
                        body = (json.dumps({"ok": False, "error": "unknown tx alias"}) + "\n").encode("utf-8")
                        writer.write(http_response("400 Bad Request", "application/json", body))
                        return
                    cmd_line = f"matrix set {tx} {rx}"
                if rx not in cfg.rx_by_alias:
                    body = (json.dumps({"ok": False, "error": "unknown rx alias"}) + "\n").encode("utf-8")
                    writer.write(http_response("400 Bad Request", "application/json", body))
                    return
                if rx in cfg.rx_skipped_aliases:
                    body = (json.dumps({"ok": False, "error": "rx is skipped"}) + "\n").encode("utf-8")
                    writer.write(http_response("400 Bad Request", "application/json", body))
                    return
                if tx.upper() != "NULL" and tx in cfg.tx_skipped_aliases:
                    body = (json.dumps({"ok": False, "error": "tx is skipped"}) + "\n").encode("utf-8")
                    writer.write(http_response("400 Bad Request", "application/json", body))
                    return
                outcome = await process_matrix_set_line(
                    cfg,
                    amx,
                    state,
                    cmd_line,
                    rt["amx_verify_timeout_ms"],
                    runtime,
                    notifier,
                    status_reporter,
                )
                failed_rx = {f[0] for f in outcome.failures}
                http_ok = (
                    outcome.rti_ok
                    and not outcome.exception_occurred
                    and rx not in failed_rx
                    and outcome.rti_response != "unknown command"
                )
                if not http_ok:
                    err = "matrix set failed"
                    if outcome.exception_occurred:
                        err = "matrix routing failed"
                    elif outcome.rti_response == "unknown command":
                        err = "invalid matrix command"
                    elif rx in failed_rx:
                        err = "AMX route failed for this decoder"
                    status = "500 Internal Server Error" if outcome.exception_occurred else "400 Bad Request"
                    body = (json.dumps({"ok": False, "error": err}) + "\n").encode("utf-8")
                    writer.write(http_response(status, "application/json", body))
                    LOG.warning("HTTP -> %s", cmd_line)
                    return
                video_tx = state.video.get(rx)
                body = (json.dumps({"ok": True, "rx": rx, "video_tx": video_tx}) + "\n").encode("utf-8")
                writer.write(http_response("200 OK", "application/json", body))
                LOG.info("HTTP -> %s", cmd_line)
                return

            if path.startswith("/control/restart"):
                if params_want_html(params):
                    writer.write(
                        http_response(
                            "200 OK",
                            "text/html; charset=utf-8",
                            control_feedback_html(
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
                    writer.write(http_response("200 OK", "application/json", body))
                LOG.warning("HTTP control [source=%s]: service restart requested", ctl_via)
                asyncio.create_task(do_service_restart(reason=f"http_control:{ctl_via}"))
                return

            if path.startswith("/control/reboot"):
                if params_want_html(params):
                    writer.write(
                        http_response(
                            "200 OK",
                            "text/html; charset=utf-8",
                            control_feedback_html(
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
                    writer.write(http_response("200 OK", "application/json", body))
                LOG.warning("HTTP control [source=%s]: host reboot requested", ctl_via)
                asyncio.create_task(do_reboot(reason=f"http_control:{ctl_via}"))
                return

            writer.write(http_response("404 Not Found", "text/plain", b"not found"))
            return

        # Same HTML for /, /home, /controls, /status, /matrix — one <section> shown per path.
        if path_only in ("/", "/home", "/controls", "/status", "/matrix") or path.startswith("/?"):
            initial_page = "home"
            if path_only == "/controls":
                initial_page = "controls"
            elif path_only == "/status":
                initial_page = "status"
            elif path_only == "/matrix":
                initial_page = "matrix"

            def _page_hidden(name: str) -> str:
                return "" if initial_page == name else " hidden"

            def _nav_active(name: str) -> str:
                return " dt-nav-active" if initial_page == name else ""

            uptime_h = format_uptime(int(snapshot["uptime_seconds"]))
            amx_conn = (
                f"{snapshot['amx_connected']}/{max(snapshot['amx_total_known'] or 0, snapshot['rx_configured'])}"
                if snapshot["amx_connected"] is not None
                else "n/a"
            )
            log_lines = "\n".join(get_log_tail(rt["http_log_lines"]))
            tx_aliases = sorted(cfg.tx_by_alias.keys(), key=tx_alias_sort_key)
            rx_aliases = sorted(cfg.rx_by_alias.keys(), key=rx_alias_sort_key)
            tx_start_ip = cfg.tx_by_alias[tx_aliases[0]].ip if tx_aliases else ""
            rx_start_ip = cfg.rx_by_alias[rx_aliases[0]].ip if rx_aliases else ""
            endpoint_inventory = load_endpoint_inventory(config_path=config_path)
            tx_skip_by_alias = {
                str(row.get("alias", "")): as_bool(row.get("skip"), default=False)
                for row in endpoint_inventory.get("tx", [])
            }
            rx_skip_by_alias = {
                str(row.get("alias", "")): as_bool(row.get("skip"), default=False)
                for row in endpoint_inventory.get("rx", [])
            }
            route_rows = []
            tx_all_aliases = sorted(
                set(tx_aliases).union({a for a in tx_skip_by_alias.keys() if a}),
                key=tx_alias_sort_key,
            )
            rx_all_aliases = sorted(
                set(rx_aliases).union({a for a in rx_skip_by_alias.keys() if a}),
                key=rx_alias_sort_key,
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
                        f"<tr><td><code>{html.escape(tx_alias)}</code></td><td><code>-</code></td><td class=\"bad\"><b>SKIPPED</b></td><td><b>-</b></td><td><b>-</b></td><td>{tx_skip_btn}</td></tr>"
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
                signal_txt, signal_cls = format_tx_signal(tx_fields)
                route_rows.append(
                    f"<tr><td><code>{tx_alias}</code></td><td><code>{stream_txt}</code></td><td class=\"{tx_status_cls}\"><b>{tx_status_txt}</b></td><td class=\"{signal_cls}\"><b>{html.escape(signal_txt)}</b></td><td><b>-</b></td><td>{tx_skip_btn}</td></tr>"
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
                        f"<tr><td><code>{html.escape(rx_alias)}</code></td><td><code>NULL</code></td><td class=\"bad\"><b>SKIPPED</b></td><td><b>-</b></td><td><b>-</b></td><td>{rx_skip_btn}</td></tr>"
                    )
                    continue
                if rx_alias not in cfg.rx_by_alias:
                    continue
                tx_alias = state.video.get(rx_alias) or "NULL"
                online = state.rx_online.get(rx_alias, True)
                status_txt = "ONLINE" if online else "OFFLINE"
                status_cls = "ok" if online else "bad"
                hdmi_enabled = state.rx_hdmi_output.get(rx_alias)
                hdmi_link = state.rx_hdmi_link.get(rx_alias)
                if hdmi_enabled is True:
                    hdmi_txt = "ON"
                    hdmi_cls = "ok"
                elif hdmi_enabled is False:
                    hdmi_txt = "OFF"
                    hdmi_cls = "bad"
                else:
                    hdmi_txt = "UNKNOWN"
                    hdmi_cls = ""
                if hdmi_link is True:
                    hdmi_link_txt = "CONNECTED"
                    hdmi_link_cls = "ok"
                elif hdmi_link is False:
                    hdmi_link_txt = "DISCONNECTED"
                    hdmi_link_cls = "bad"
                else:
                    hdmi_link_txt = "UNKNOWN"
                    hdmi_link_cls = ""
                route_rows.append(
                    f"<tr><td><code>{rx_alias}</code></td><td><code>{tx_alias}</code></td><td class=\"{status_cls}\"><b>{status_txt}</b></td><td class=\"{hdmi_cls}\"><b>{hdmi_txt}</b></td><td class=\"{hdmi_link_cls}\"><b>{hdmi_link_txt}</b></td><td>{rx_skip_btn}</td></tr>"
                )
            route_html = "\n".join(route_rows)

            mtx_cols = sorted(cfg.tx_by_alias.keys(), key=tx_alias_sort_key)
            mrx_rows = sorted(cfg.rx_by_alias.keys(), key=rx_alias_sort_key)
            matrix_rows_parts: List[str] = []
            if mrx_rows:
                show_none_col = any(state.video.get(rx) is None for rx in mrx_rows)
                th_tx = "".join(
                    f'<th scope="col" title="{html.escape(tx)}">{html.escape(tx)}</th>' for tx in mtx_cols
                )
                th_all = (
                    '<th scope="col" title="No input (NULL) — status only; use RTI to assign NULL">None</th>'
                    if show_none_col
                    else ""
                )
                for rx in mrx_rows:
                    rx_skip = rx in cfg.rx_skipped_aliases
                    cur = state.video.get(rx)
                    tds: List[str] = [f'<th scope="row"><code>{html.escape(rx)}</code></th>']
                    for tx in mtx_cols:
                        tx_skip = tx in cfg.tx_skipped_aliases
                        disabled = rx_skip or tx_skip
                        is_on = not disabled and cur == tx
                        btn_cls = "dt-matrix-cell"
                        if is_on:
                            btn_cls += " dt-matrix-on"
                        if disabled:
                            btn_cls += " dt-matrix-skip"
                        dis = " disabled" if disabled else ""
                        tds.append(
                            f'<td><button type="button" class="{btn_cls}" data-matrix-rx="{html.escape(rx)}" '
                            f'data-matrix-tx="{html.escape(tx)}"{dis} '
                            f'aria-label="Route {html.escape(rx)} to {html.escape(tx)}"></button></td>'
                        )
                    if show_none_col:
                        is_on_n = not rx_skip and cur is None
                        btn_cls = "dt-matrix-cell dt-matrix-none"
                        if is_on_n:
                            btn_cls += " dt-matrix-on"
                        if rx_skip:
                            btn_cls += " dt-matrix-skip"
                        tds.append(
                            f'<td><button type="button" class="{btn_cls}" data-matrix-rx="{html.escape(rx)}" '
                            f'data-matrix-tx="NULL" disabled '
                            f'aria-label="No input (NULL) for {html.escape(rx)}"></button></td>'
                        )
                    matrix_rows_parts.append("<tr>" + "".join(tds) + "</tr>")
                matrix_table_html = (
                    '<div class="table-wrap dt-matrix-wrap"><table class="dt-matrix-table" role="grid">'
                    "<thead><tr><th scope=\"col\">RX</th>"
                    + th_tx
                    + th_all
                    + "</tr></thead><tbody>"
                    + "\n".join(matrix_rows_parts)
                    + "</tbody></table></div>"
                )
            else:
                matrix_table_html = '<p class="subtle">No RX endpoints configured.</p>'

            _ct = cfg.http_status_control_token or ""
            ctl_qs = ("&token=" + urllib.parse.quote_plus(_ct)) if _ct else ""
            _ui_sess = http_ui_sess_issue()
            _ui_sess_js = json.dumps(_ui_sess)
            _ctl_qs_js = json.dumps(ctl_qs)
            _amx_port = int(cfg.amx_decoder_port)
            _av_bind_ok, _av_bind_msg = check_av_bind_address(cfg.amx_bind_address)
            _av_default_range = infer_default_scan_range(cfg)
            _av_sug = next_suggested_aliases(cfg)
            _av_scan_js = json.dumps(
                {
                    "defaultRange": _av_default_range,
                    "bindOk": _av_bind_ok,
                    "bindMsg": _av_bind_msg,
                    "dryRun": cfg.amx_dry_run,
                    "bindAddress": cfg.amx_bind_address,
                    "suggestedNextTx": _av_sug["next_tx_alias"],
                    "suggestedNextRx": _av_sug["next_rx_alias"],
                }
            )
            _unknown_pre = html.escape(unknown_ctl_page_text())
            _uc_path = unknown_ctl_persist_file()
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
            _h_title_controls = html.escape(
                "Control changes are saved to config and survive restart/reboot. "
                "Each control below has its own ? with full help. Results open in a short on-page message."
            )
            _h_title_devices = html.escape(
                f"TX rows are polled from AMX getStatus every {TX_STATUS_POLL_INTERVAL_SECONDS}s. "
                f"RX rows can be polled from AMX getStatus every {RX_STATUS_POLL_INTERVAL_SECONDS}s (toggle in Controls). "
                "RX HDMI output state uses HDMIOFF or DVIOFF; HDMI link state uses HDMISTATUS or DVISTATUS. "
                "Skip toggles are saved to config and apply after restart."
            )
            _h_title_logs = html.escape(
                "Tail of the service log on this host. Refreshes when this tab auto-updates (every 5s while visible)."
            )
            _unknown_tooltip = (
                "Lines the WyreStorm driver sent on the RTI TCP port that returned unknown command (or similar). "
                "Identical lines are merged; the number is how many times each was sent. Times are UTC. "
                "Select the log below and copy for support. "
            )
            if _uc_path is not None:
                _unknown_tooltip += f"When persistence is enabled, the list is stored at {str(_uc_path)}."
            else:
                _unknown_tooltip += (
                    "Not persisted when unknown_ctl.enabled is false in config; restart clears the list."
                )
            _h_title_unknown = html.escape(_unknown_tooltip)
            _h_title_matrix = html.escape(
                "Click a TX cell to assign that RX (same as matrix set on the RTI port). "
                "The None column appears only when at least one RX has no input; it shows status and cannot set NULL here (use RTI for matrix set NULL). "
                "Skipped endpoints are disabled."
            )
            body = f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <link rel="dns-prefetch" href="//fonts.googleapis.com"/>
  <link rel="preconnect" href="https://fonts.googleapis.com"/>
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin/>
  <link rel="stylesheet" href="https://fonts.googleapis.com/css?family=Karla:400,600,700%7CRubik:600,700&display=swap"/>
  <title>Conductor1</title>
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
      font-family: "Karla", system-ui, -apple-system, "Segoe UI", Roboto, Arial, sans-serif;
      margin: 0;
      min-height: 100vh;
      background: var(--bg);
      color: var(--fg);
      line-height: 1.5;
    }}
    .wrap {{
      max-width: 1024px;
      margin: 0 auto;
      padding: 28px 20px 48px;
    }}
    a {{ color: var(--link); text-decoration: none; }}
    a:hover {{ text-decoration: underline; }}

    .dt-menubar {{
      background: var(--card);
      border-bottom: 1px solid var(--border);
      box-shadow: var(--shadow);
      margin-bottom: 24px;
    }}
    .dt-menubar-inner {{
      max-width: 1024px;
      margin: 0 auto;
      padding: 12px 20px;
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      justify-content: space-between;
      gap: 12px 20px;
    }}
    .dt-menubar-brand {{
      display: inline-flex;
      flex-direction: row;
      align-items: center;
      line-height: 1.12;
      font-family: Cambria, "Palatino Linotype", Palatino, Georgia, "Times New Roman", serif;
      color: var(--fg);
      text-decoration: none;
      gap: 10px;
      min-width: 0;
    }}
    .dt-brand-logo {{
      display: block;
      height: 51px;
      width: auto;
      max-width: min(360px, 72vw);
      object-fit: contain;
    }}
    .dt-brand-logo.dt-brand-logo-footer {{
      height: 22px;
      max-width: min(180px, 60vw);
    }}
    .dt-sr-only {{
      position: absolute;
      width: 1px;
      height: 1px;
      padding: 0;
      margin: -1px;
      overflow: hidden;
      clip: rect(0, 0, 0, 0);
      white-space: nowrap;
      border: 0;
    }}
    .dt-menubar-brand:hover {{ color: var(--link); text-decoration: none; }}
    .dt-footer {{
      margin-top: 36px;
      padding-top: 20px;
      border-top: 1px solid var(--border);
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 14px;
      flex-wrap: wrap;
    }}
    .dt-powered {{
      display: inline-flex;
      align-items: center;
      gap: 10px;
      color: var(--muted);
      font-size: 13px;
    }}

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
      font-family: "Rubik", system-ui, -apple-system, "Segoe UI", Roboto, Arial, sans-serif;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      color: var(--muted);
      margin: 28px 0 10px 0;
      border-left: 3px solid var(--accent);
      padding-left: 10px;
    }}
    .section-title:first-of-type {{ margin-top: 0; }}

    .dt-page-title-row {{
      display: flex;
      justify-content: space-between;
      align-items: flex-start;
      gap: 12px;
      margin: 28px 0 10px 0;
    }}
    .dt-page-title-row:first-child {{ margin-top: 0; }}
    .dt-page-title-row .section-title {{ margin: 0; }}
    .dt-page-help {{
      flex-shrink: 0;
      margin-top: 1px;
      cursor: help;
    }}

    code {{
      background: var(--code-bg);
      color: var(--code-fg);
      padding: 3px 8px;
      border-radius: 6px;
      font-size: 0.88em;
    }}
    .ok {{ color: #16a34a; font-weight: 600; }}
    .bad {{ color: #dc2626; font-weight: 600; }}

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

    .row.row-system-size {{
      flex-direction: column;
      align-items: stretch;
      gap: 12px;
    }}
    .row.row-system-size > .system-size-heading {{
      padding-bottom: 0;
    }}
    .system-size-grid {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 12px 16px;
      align-items: end;
      width: 100%;
    }}
    .sf-item {{
      display: flex;
      flex-direction: column;
      gap: 4px;
      min-width: 0;
    }}
    .sf-item label {{
      font-size: 12px;
      color: var(--muted);
      font-weight: 500;
    }}
    .sf-item input {{
      width: 100%;
      min-width: 0;
    }}
    .sf-apply {{
      grid-column: 1 / -1;
      display: flex;
      align-items: flex-end;
      justify-content: flex-start;
      padding-top: 2px;
    }}
    @media (min-width: 720px) {{
      .system-size-grid {{
        grid-template-columns: 88px minmax(140px, 1fr) 88px minmax(140px, 1fr) auto;
      }}
      .sf-apply {{
        grid-column: auto;
        align-self: end;
      }}
    }}
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
    .av-scan-card h2 {{ font-size: 1.15rem; }}
    .av-scan-table th, .av-scan-table td {{ padding: 6px 8px; text-align: left; border-bottom: 1px solid var(--border); }}
    .av-scan-table th {{ color: var(--muted); font-weight: 600; font-size: 12px; }}
    .av-scan-table tr:last-child td {{ border-bottom: none; }}
    .av-scan-card {{ position: relative; }}
    .av-scan-busy[hidden] {{ display: none !important; }}
    .av-scan-busy:not([hidden]) {{
      position: absolute; inset: 0; z-index: 25; border-radius: 16px;
      display: flex; align-items: center; justify-content: center;
      background: rgba(15, 23, 42, 0.72);
      backdrop-filter: blur(2px);
    }}
    .av-scan-busy-inner {{ text-align: center; padding: 0 20px; max-width: 320px; }}
    .av-scan-busy-title {{ margin: 0 0 6px 0; font-size: 1.15rem; font-weight: 700; color: #f8fafc; }}
    .av-scan-busy-sub {{ margin: 0 0 16px 0; font-size: 13px; color: #cbd5e1; }}
    .av-scan-spinner {{
      width: 36px; height: 36px; margin: 0 auto 14px;
      border: 3px solid rgba(255,255,255,0.25); border-top-color: #93c5fd;
      border-radius: 50%; animation: av-spin 0.75s linear infinite;
    }}
    @keyframes av-spin {{ to {{ transform: rotate(360deg); }} }}

    .dt-nav {{
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      justify-content: flex-end;
      gap: 4px;
      margin: 0;
      padding: 0;
      list-style: none;
      flex: 1;
      min-width: 0;
      font-family: "Rubik", system-ui, -apple-system, "Segoe UI", Roboto, Arial, sans-serif;
      letter-spacing: 0.04em;
    }}
    .dt-nav-link {{
      display: inline-block;
      font-weight: 650;
      font-size: 0.84rem;
      color: var(--muted);
      text-decoration: none;
      padding: 8px 14px;
      border-radius: 8px;
      border: 1px solid transparent;
      text-transform: uppercase;
    }}
    .dt-nav-link:hover {{
      color: var(--link);
      background: var(--row);
      text-decoration: none;
    }}
    .dt-nav-link.dt-nav-active {{
      color: var(--link);
      background: var(--accent-soft);
      border-color: var(--border);
      text-decoration: none;
    }}
    .dt-matrix-wrap {{ overflow: auto; max-width: 100%; margin: 0 -4px; }}
    .dt-matrix-table {{ border-collapse: collapse; font-size: 0.82rem; width: max-content; min-width: 100%; }}
    .dt-matrix-table th, .dt-matrix-table td {{
      border: 1px solid var(--border); padding: 4px 6px; text-align: center; vertical-align: middle;
    }}
    .dt-matrix-table th[scope="row"] {{ text-align: left; white-space: nowrap; }}
    .dt-matrix-cell {{
      width: 44px; height: 36px; min-width: 44px; padding: 0; box-sizing: border-box;
      border: 1px solid var(--border); border-radius: 6px; background: var(--row); cursor: pointer;
    }}
    .dt-matrix-cell.dt-matrix-on {{
      background: #16a34a; border-color: #15803d; cursor: default;
    }}
    [data-theme="dark"] .dt-matrix-cell.dt-matrix-on {{
      background: #15803d; border-color: #166534;
    }}
    .dt-matrix-cell.dt-matrix-skip {{ opacity: 0.4; cursor: not-allowed; }}
    .dt-matrix-cell.dt-matrix-none {{ cursor: default; }}
    .dt-matrix-cell.dt-matrix-busy {{ opacity: 0.55; pointer-events: none; }}
    .dt-page[hidden] {{ display: none !important; }}
    .dt-page > .section-title:first-child,
    .dt-page > .dt-page-title-row:first-child {{ margin-top: 0; }}
  </style>
</head>
<body>
  <header class="dt-menubar">
    <div class="dt-menubar-inner">
      <a href="/home" class="dt-menubar-brand" aria-label="Home">
        <img class="dt-brand-logo" src="/assets/conductor1.png" alt="Conductor1" />
        <span class="dt-sr-only">Home</span>
      </a>
      <nav class="dt-nav" aria-label="Main">
        <a href="/home" class="dt-nav-link{_nav_active('home')}">Home</a>
        <a href="/controls" class="dt-nav-link{_nav_active('controls')}">Controls</a>
        <a href="/status" class="dt-nav-link{_nav_active('status')}">Status</a>
        <a href="/matrix" class="dt-nav-link{_nav_active('matrix')}">Matrix</a>
      </nav>
    </div>
  </header>

  <div class="wrap">
  <section id="page-home" class="dt-page"{_page_hidden('home')}>
  <div class="section-title">Overview</div>
  <div class="card" id="homeOverviewCard">
    <div class="row"><div>Status</div><div><span class="ok">Service online</span></div></div>
    <div class="row"><div>Uptime</div><div><code>{uptime_h}</code></div></div>
    <div class="row"><div>Mode</div><div><code>{snapshot['mode']}</code></div></div>
    <div class="row"><div>RTI clients</div><div><code>{snapshot['rti_clients']}</code></div></div>
    <div class="row"><div>Configured TX</div><div><code id="st_tx_configured">{snapshot['tx_configured']}</code></div></div>
    <div class="row"><div>Configured RX</div><div><code id="st_rx_configured">{snapshot['rx_configured']}</code></div></div>
    <div class="row"><div>AMX connections</div><div><code>{amx_conn}</code></div></div>
  </div>
  </section>

  <section id="page-controls" class="dt-page"{_page_hidden('controls')}>
  <div class="dt-page-title-row">
    <div class="section-title">Controls</div>
    <span class="help-icon dt-page-help" title="{_h_title_controls}">?</span>
  </div>
  <div class="card">
    <div class="row">
      <div><b>AMX dry-run mode</b><span class="help-icon" title="When ON, no AMX TCP connections are made and decoder state is simulated. Saved to config; restart required to apply.">?</span></div>
      <div class="ctrl-actions">
        <code id="st_amx_dry_run">{str(rt.get('amx_dry_run', cfg.amx_dry_run)).lower()}</code>
        <button type="button" class="ctrl-run" data-dt-ctl="set" data-key="amx_dry_run" data-value="{'false' if rt.get('amx_dry_run', cfg.amx_dry_run) else 'true'}">Toggle</button>
      </div>
    </div>
    <div class="row">
      <div><b>AMX verify after switch</b><span class="help-icon" title="When ON, after each route the translator asks each affected decoder for STREAM via AMX and logs mismatches locally. RTI still gets an immediate matrix ack. This verify step is automatically inactive while AMX RX polling is ON.">?</span></div>
      <div class="ctrl-actions">
        <code id="st_amx_verify">{str(rt['amx_verify_after_set']).lower()}{' (inactive: RX polling on)' if (rt.get('amx_rx_poll_enabled', True) and rt['amx_verify_after_set']) else ''}</code>
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
      <div><b>AMX TX polling</b><span class="help-icon" title="When ON, polls each active TX with AMX getStatus every {TX_STATUS_POLL_INTERVAL_SECONDS}s and updates TX online/signal state. Runtime setting (also saved to config).">?</span></div>
      <div class="ctrl-actions">
        <code id="st_amx_tx_poll">{str(rt.get('amx_tx_poll_enabled', True)).lower()}</code>
        <button type="button" class="ctrl-run" data-dt-ctl="set" data-key="amx_tx_poll_enabled" data-value="{'false' if rt.get('amx_tx_poll_enabled', True) else 'true'}">Toggle</button>
      </div>
    </div>
    <div class="row">
      <div><b>AMX RX polling</b><span class="help-icon" title="When ON, polls each active RX with AMX getStatus every {RX_STATUS_POLL_INTERVAL_SECONDS}s and updates online/route/HDMI state. Runtime setting (also saved to config).">?</span></div>
      <div class="ctrl-actions">
        <code id="st_amx_rx_poll">{str(rt.get('amx_rx_poll_enabled', True)).lower()}</code>
        <button type="button" class="ctrl-run" data-dt-ctl="set" data-key="amx_rx_poll_enabled" data-value="{'false' if rt.get('amx_rx_poll_enabled', True) else 'true'}">Toggle</button>
      </div>
    </div>
    <div class="row">
      <div><b>RTI status telemetry</b><span class="help-icon" title="When ON, sends a full DTTX/DTRXSUMMARY snapshot when RTI connects to the Two Way TCP port, then only changed lines (AMX poll / matrix). No periodic refresh.">?</span></div>
      <div class="ctrl-actions">
        <code id="st_rti_status">{str(rt.get('rti_status_enabled', False)).lower()}</code>
        <button type="button" class="ctrl-run" data-dt-ctl="set" data-key="rti_status_enabled" data-value="{'false' if rt.get('rti_status_enabled', False) else 'true'}">Toggle</button>
      </div>
    </div>
    <div class="row row-system-size">
      <div class="system-size-heading"><b>System size (TX/RX + starting IPs)</b><span class="help-icon" title="Regenerates endpoints.tx/endpoints.rx using installer naming and sequential IPs. TX uses INn-BOXn, stream=n, hostname NHD-120-TX-000...n. RX uses OUTn-TVn, hostname NHD-120-RX-000...(100+n), and amx_decoder_ip = RX IP. Saved to config; restart required.">?</span></div>
      <div class="system-size-grid">
        <div class="sf-item">
          <label for="txCount">TX</label>
          <input id="txCount" class="btn" style="padding:6px 8px;" type="number" min="1" max="512" step="1" value="{snapshot['tx_configured']}"/>
        </div>
        <div class="sf-item">
          <label for="txStartIp">TX start IP</label>
          <input id="txStartIp" class="btn" style="padding:6px 8px;" type="text" value="{html.escape(tx_start_ip)}"/>
        </div>
        <div class="sf-item">
          <label for="rxCount">RX</label>
          <input id="rxCount" class="btn" style="padding:6px 8px;" type="number" min="1" max="512" step="1" value="{snapshot['rx_configured']}"/>
        </div>
        <div class="sf-item">
          <label for="rxStartIp">RX start IP</label>
          <input id="rxStartIp" class="btn" style="padding:6px 8px;" type="text" value="{html.escape(rx_start_ip)}"/>
        </div>
        <div class="sf-apply">
          <button id="applyEndpointSizing" class="btn btn-primary ctrl-run" type="button" data-dt-ctl="set_endpoints">Apply</button>
        </div>
      </div>
    </div>
    <div class="row">
      <div><b>AMX self-test</b><span class="help-icon" title="Short TCP connect to every configured decoder IP. In dry-run mode all decoders count as reachable without connecting.">?</span></div>
      <div class="ctrl-actions"><button type="button" class="ctrl-run" data-dt-ctl="selftest">Run now</button></div>
    </div>
    <div class="row">
      <div><b>Scan AV network</b><span class="help-icon" title="Probes each IPv4 in the range on the AMX TCP port and classifies encoders vs decoders from AMX status. Requires amx.bind_address to be usable on this host when set. Not available when AMX dry-run is enabled. Avoid during opening hours — probing can cause AMX control reconnects. New devices are merged into endpoints (restart required).">?</span></div>
      <div class="ctrl-actions"><button type="button" class="ctrl-run" id="avScanOpenBtn">Scan…</button></div>
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
  </section>

  <section id="page-status" class="dt-page"{_page_hidden('status')}>
  <div class="dt-page-title-row">
    <div class="section-title">Devices</div>
    <span class="help-icon dt-page-help" title="{_h_title_devices}">?</span>
  </div>
  <div class="table-wrap">
  <table>
    <thead><tr><th>Endpoint</th><th>Route / Stream</th><th>Status</th><th>HDMI Out</th><th>HDMI Link</th><th>Skip</th></tr></thead>
    <tbody id="devicesBody">
      {route_html}
    </tbody>
  </table>
  </div>

  <div class="dt-page-title-row">
    <div class="section-title">Recent logs</div>
    <span class="help-icon dt-page-help" title="{_h_title_logs}">?</span>
  </div>
  <pre id="logsPre">{log_lines}</pre>

  <div class="dt-page-title-row">
    <div class="section-title">Unrecognized RTI commands</div>
    <span class="help-icon dt-page-help" title="{_h_title_unknown}">?</span>
  </div>
  <pre id="unknownCtlPre" style="max-height:320px;overflow-y:auto;font-size:11px;">{_unknown_pre}</pre>
  <div class="unk-ctl-actions">
    <button type="button" class="ctrl-run" data-dt-ctl="copy_unknown_ctl">Copy all</button>
    <button type="button" class="ctrl-run" data-dt-ctl="clear_unknown_ctl">Clear unrecognized list</button>
    <span class="subtle" style="display:block;margin-top:8px;margin-bottom:0;">Wipes this list and the on-disk file (when persistence is enabled). Page reloads after confirm.</span>
  </div>
  </section>

  <section id="page-matrix" class="dt-page"{_page_hidden('matrix')}>
  <div class="dt-page-title-row">
    <div class="section-title">Matrix</div>
    <span class="help-icon dt-page-help" title="{_h_title_matrix}">?</span>
  </div>
  <div class="card" id="matrixCard">
    {matrix_table_html}
  </div>
  </section>

  <div id="dtModal" class="dt-modal" hidden>
    <div class="dt-modal-backdrop" id="dtModalBackdrop"></div>
    <div class="dt-modal-card" id="dtModalCard" role="dialog" aria-modal="true" aria-labelledby="dtModalTitle">
      <h2 id="dtModalTitle"></h2>
      <p id="dtModalText"></p>
      <button type="button" class="btn btn-primary" id="dtModalOk">OK</button>
    </div>
  </div>

  <div id="avScanModal" class="dt-modal" hidden>
    <div class="dt-modal-backdrop" id="avScanBackdrop"></div>
    <div class="dt-modal-card av-scan-card" role="dialog" aria-modal="true" aria-labelledby="avScanTitle" style="max-width: 640px;">
      <h2 id="avScanTitle">Scan AV network</h2>
      <p id="avScanBindLine" class="subtle" style="margin-top:0"></p>
      <div class="sf-item" style="margin-bottom:10px">
        <label for="avScanRange">IPv4 range (CIDR or start–end)</label>
        <input id="avScanRange" class="btn" style="padding:6px 8px;width:100%;box-sizing:border-box" type="text" />
        <p class="subtle" style="margin:8px 0 0 0;font-size:12px;line-height:1.55">
          <b>Examples</b>
          — subnet: <code>192.168.10.0/24</code>
          · inclusive range (both ends must be full IPv4 addresses):
          <code>10.16.26.110-10.16.26.120</code>
          · single host: <code>192.168.10.50</code>
          · max 4096 addresses per scan.
        </p>
      </div>
      <div class="ctrl-actions" style="margin-bottom:12px">
        <button type="button" class="ctrl-run" id="avScanRunBtn">Run scan</button>
        <button type="button" class="btn" id="avScanCloseBtn">Close</button>
      </div>
      <p id="avScanStatus" class="subtle" style="min-height:1.2em;margin-bottom:8px"></p>
      <div style="overflow:auto;max-height:280px;border:1px solid var(--border);border-radius:8px">
        <table class="av-scan-table" style="width:100%;font-size:13px;border-collapse:collapse">
          <thead><tr><th style="width:36px"></th><th>IP</th><th>Role</th><th>Note</th></tr></thead>
          <tbody id="avScanTbody"></tbody>
        </table>
      </div>
      <p id="avScanHint" class="subtle" style="margin-top:10px;margin-bottom:0;font-size:12px"></p>
      <div class="ctrl-actions" style="margin-top:12px">
        <button type="button" class="btn btn-primary ctrl-run" id="avScanAddBtn" disabled>Add selected to project</button>
      </div>
      <div id="avScanBusyOverlay" class="av-scan-busy" hidden>
        <div class="av-scan-busy-inner">
          <div class="av-scan-spinner" aria-hidden="true"></div>
          <p class="av-scan-busy-title">Scanning…</p>
          <p class="av-scan-busy-sub">Probing up to 48 addresses at a time. This can take a while on large ranges.</p>
          <button type="button" class="btn" id="avScanAbortBtn" style="background:var(--card);color:var(--fg);border:1px solid var(--border)">Abort</button>
        </div>
      </div>
    </div>
  </div>

  <div class="dt-footer">
    <div class="dt-powered">
      <span>Powered by</span>
      <img class="dt-brand-logo dt-brand-logo-footer" src="/assets/integrion_logo.png" alt="Integrion" />
    </div>
    <button class="btn" id="themeBtn" type="button">Theme</button>
  </div>
  </div>
  <script>
    (function () {{
      const DT_UI = {{ sess: {_ui_sess_js}, ctl: {_ctl_qs_js}, port: {_amx_port} }};
      const DT_AV = {_av_scan_js};
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
          return 'AMX persistent mode is disabled in this build. Non-persistent mode remains active.';
        if (key === 'amx_verify_timeout_ms')
          return 'Verify timeout is now ' + j.amx_verify_timeout_ms + ' ms. Applies immediately; no restart.';
        if (key === 'amx_verify_after_set')
          return 'AMX verify after switch is now ' + String(j.amx_verify_after_set).toLowerCase() + '. Applies immediately (inactive while RX polling is ON).';
        if (key === 'expanded_log')
          return 'Expanded log is now ' + String(j.expanded_log).toLowerCase() + '. Applies immediately.';
        if (key === 'amx_tx_poll_enabled')
          return 'AMX TX polling is now ' + String(j.amx_tx_poll_enabled).toLowerCase() + '. Applies immediately.';
        if (key === 'amx_rx_poll_enabled')
          return 'AMX RX polling is now ' + String(j.amx_rx_poll_enabled).toLowerCase() + '. Applies immediately' + (j.amx_rx_poll_enabled && j.amx_verify_after_set ? '; route verify is currently inactive.' : '.');
        if (key === 'rti_status_enabled')
          return 'RTI status telemetry is now ' + String(j.rti_status_enabled).toLowerCase() + '. Applies immediately.';
        return 'Setting updated. Applies immediately.';
      }}

      function updateVerifyStatus(j) {{
        const el = document.getElementById('st_amx_verify');
        if (!el || !j) return;
        const verifyOn = !!j.amx_verify_after_set;
        const rxPollOn = !!j.amx_rx_poll_enabled;
        el.textContent = String(verifyOn).toLowerCase() + ((verifyOn && rxPollOn) ? ' (inactive: RX polling on)' : '');
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
                updateVerifyStatus(j);
                btn.setAttribute('data-value', j.amx_verify_after_set ? 'false' : 'true');
              }}
              if (key === 'amx_dry_run') {{
                const el = document.getElementById('st_amx_dry_run');
                if (el) el.textContent = String(j.amx_dry_run).toLowerCase();
                btn.setAttribute('data-value', j.amx_dry_run ? 'false' : 'true');
              }}
              if (key === 'expanded_log') {{
                const el = document.getElementById('st_expanded_log');
                if (el) el.textContent = String(j.expanded_log).toLowerCase();
                btn.setAttribute('data-value', j.expanded_log ? 'false' : 'true');
              }}
              if (key === 'amx_rx_poll_enabled') {{
                const el = document.getElementById('st_amx_rx_poll');
                if (el) el.textContent = String(j.amx_rx_poll_enabled).toLowerCase();
                btn.setAttribute('data-value', j.amx_rx_poll_enabled ? 'false' : 'true');
                updateVerifyStatus(j);
              }}
              if (key === 'amx_tx_poll_enabled') {{
                const el = document.getElementById('st_amx_tx_poll');
                if (el) el.textContent = String(j.amx_tx_poll_enabled).toLowerCase();
                btn.setAttribute('data-value', j.amx_tx_poll_enabled ? 'false' : 'true');
              }}
              if (key === 'rti_status_enabled') {{
                const el = document.getElementById('st_rti_status');
                if (el) el.textContent = String(j.rti_status_enabled).toLowerCase();
                btn.setAttribute('data-value', j.rti_status_enabled ? 'false' : 'true');
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

      (function avScanUi() {{
        const avModal = document.getElementById('avScanModal');
        const avRange = document.getElementById('avScanRange');
        const avBindLine = document.getElementById('avScanBindLine');
        const avStatus = document.getElementById('avScanStatus');
        const avTbody = document.getElementById('avScanTbody');
        const avHint = document.getElementById('avScanHint');
        const avOpen = document.getElementById('avScanOpenBtn');
        const avClose = document.getElementById('avScanCloseBtn');
        const avRun = document.getElementById('avScanRunBtn');
        const avAdd = document.getElementById('avScanAddBtn');
        const avBackdrop = document.getElementById('avScanBackdrop');
        const avBusy = document.getElementById('avScanBusyOverlay');
        const avAbort = document.getElementById('avScanAbortBtn');

        function setAvScanBusy(on) {{
          if (avBusy) avBusy.hidden = !on;
          if (avRun) avRun.disabled = !!on;
          if (avClose) avClose.disabled = !!on;
          if (avAbort) avAbort.disabled = !on;
        }}

        if (avAbort) {{
          avAbort.addEventListener('click', () => {{
            fetch(ctlUrl('/control/av_scan_abort')).catch(() => {{}});
            if (avStatus) avStatus.textContent = 'Stopping after current probe batch…';
          }});
        }}

        if (!avModal || !avRange || !avOpen) return;

        function showAvScan() {{
          if (DT_AV.dryRun) {{
            showModal(false, 'Not available', 'Not available in dry-run mode');
            return;
          }}
          avRange.value = DT_AV.defaultRange || '';
          const bindOk = !!DT_AV.bindOk;
          const b = (DT_AV.bindAddress || '').toString();
          if (b) {{
            avBindLine.textContent = (bindOk ? '✓ ' : '✗ ') + (DT_AV.bindMsg || '') +
              ' (amx.bind_address: ' + b + ')';
          }} else {{
            avBindLine.textContent = (DT_AV.bindMsg || '') + ' (no amx.bind_address)';
          }}
          avBindLine.style.color = bindOk ? 'var(--muted)' : '#b91c1c';
          if (avTbody) avTbody.innerHTML = '';
          if (avStatus) avStatus.textContent = '';
          if (avHint) {{
            avHint.textContent = 'Next suggested aliases if you add devices: TX ' + (DT_AV.suggestedNextTx || '') +
              ', RX ' + (DT_AV.suggestedNextRx || '');
          }}
          if (avAdd) avAdd.disabled = true;
          setAvScanBusy(false);
          avModal.removeAttribute('hidden');
        }}
        function hideAvScan() {{ avModal.setAttribute('hidden', ''); }}
        avOpen.addEventListener('click', showAvScan);
        if (avClose) avClose.addEventListener('click', hideAvScan);
        if (avBackdrop) avBackdrop.addEventListener('click', hideAvScan);

        function refreshAvScanAddState() {{
          if (!avTbody || !avAdd) return;
          const n = avTbody.querySelectorAll('input[type=checkbox]:checked:not(:disabled)').length;
          avAdd.disabled = n === 0;
        }}

        if (avTbody) {{
          avTbody.addEventListener('change', refreshAvScanAddState);
        }}

        if (avRun) {{
          avRun.addEventListener('click', async () => {{
            const range = (avRange && avRange.value) ? avRange.value.trim() : '';
            if (!range) {{
              showModal(false, 'Range required', 'Enter a range (see examples under the field), e.g. 192.168.10.0/24 or 10.0.0.1-10.0.0.50 with full IPv4 on both sides.');
              return;
            }}
            if (!confirm(
              'Warning: This scan opens brief TCP connections to many addresses. AMX devices often allow only one TCP session; probing can force control reconnects or failed probes. Do not run during opening hours or while the system is in use. Continue?'
            )) return;
            if (avStatus) avStatus.textContent = '';
            if (avTbody) avTbody.innerHTML = '';
            if (avAdd) avAdd.disabled = true;
            setAvScanBusy(true);
            try {{
              const r = await fetch(ctlUrl('/control/av_scan?range=' + encodeURIComponent(range)));
              const t = await r.text();
              let j = null;
              try {{ j = JSON.parse(t); }} catch (e) {{}}
              if (r.status === 401) {{
                showModal(false, 'Session expired', 'Refresh this page and sign in again, then retry.');
                return;
              }}
              if (r.status === 403) {{
                showModal(false, 'Not allowed', 'Wrong or missing control token in config.');
                return;
              }}
              if (!r.ok) {{
                showModal(false, 'Scan failed', (j && j.error) ? j.error : (t.slice(0, 400) || r.statusText));
                if (avStatus) avStatus.textContent = '';
                return;
              }}
              if (!j) {{
                showModal(false, 'Bad response', 'Could not parse JSON.');
                return;
              }}
              const found = j.found || [];
              const sc = j.scanned != null ? j.scanned : '?';
              if (avStatus) {{
                if (j.cancelled) {{
                  const done = j.completed_probes != null ? j.completed_probes : '?';
                  avStatus.textContent = 'Scan cancelled after ' + done + ' of ' + sc + ' probes. Found ' + found.length + ' device(s).';
                }} else {{
                  avStatus.textContent = 'Probed ' + sc + ' address(es), found ' + found.length + ' device(s).';
                }}
              }}
              if (avTbody) {{
                avTbody.innerHTML = '';
                found.forEach(function (row) {{
                  const tr = document.createElement('tr');
                  const dup = !!row.duplicate;
                  const kind = row.kind === 'tx' ? 'TX' : 'RX';
                  const note = (row.duplicate_note || (dup ? 'Already in project' : '')) || '';
                  const ip = row.ip || '';
                  tr.innerHTML =
                    '<td><input type="checkbox" data-kind="' + (row.kind || '') + '" data-ip="' +
                    ip.replace(/"/g, '') + '" ' + (dup ? 'disabled title="Already in project"' : '') + '/></td>' +
                    '<td><code>' + ip + '</code></td><td>' + kind + '</td><td>' +
                    (note.replace(/</g, '&lt;')) + '</td>';
                  avTbody.appendChild(tr);
                }});
              }}
              refreshAvScanAddState();
              if (avHint && j.suggested_aliases) {{
                const stx = j.suggested_aliases.next_tx_alias || '';
                const srx = j.suggested_aliases.next_rx_alias || '';
                avHint.textContent = 'Suggested next aliases: TX ' + stx + ', RX ' + srx;
              }}
            }} catch (e) {{
              showModal(false, 'Network error', String(e.message || e));
            }} finally {{
              setAvScanBusy(false);
            }}
          }});
        }}

        if (avAdd) {{
          avAdd.addEventListener('click', async () => {{
            const tx = [];
            const rx = [];
            if (avTbody) {{
              avTbody.querySelectorAll('input[type=checkbox]:checked:not(:disabled)').forEach(function (cb) {{
                const k = cb.getAttribute('data-kind');
                const ip = cb.getAttribute('data-ip');
                if (!ip) return;
                if (k === 'tx') tx.push(ip);
                else if (k === 'rx') rx.push(ip);
              }});
            }}
            if (!tx.length && !rx.length) return;
            if (!confirm('Add ' + tx.length + ' TX and ' + rx.length + ' RX to config? Restart DriverTranslator to apply.')) return;
            setBusy(true);
            try {{
              const r = await fetch(ctlUrl(
                '/control/add_av_endpoints?tx_ips=' + encodeURIComponent(tx.join(',')) +
                '&rx_ips=' + encodeURIComponent(rx.join(','))
              ));
              const t = await r.text();
              let j = null;
              try {{ j = JSON.parse(t); }} catch (e) {{}}
              if (r.status === 401) {{
                showModal(false, 'Session expired', 'Refresh this page and sign in again, then retry.');
                return;
              }}
              if (r.status === 403) {{
                showModal(false, 'Not allowed', 'Wrong or missing control token in config.');
                return;
              }}
              if (!r.ok) {{
                showModal(false, 'Add failed', (j && j.error) ? j.error : (t.slice(0, 400) || r.statusText));
                return;
              }}
              let detail = 'Restart DriverTranslator to load new endpoints.';
              if (j) {{
                const at = (j.added_tx || []).length, ar = (j.added_rx || []).length;
                const sk = j.skipped || [];
                detail = 'Added TX: ' + at + ', RX: ' + ar + '.';
                if (sk.length) detail += ' Skipped: ' + sk.join('; ') + '.';
                detail += ' Restart DriverTranslator to apply.';
              }}
              showModal(true, 'Endpoints updated', detail);
              hideAvScan();
              await refreshLiveSections();
            }} catch (e) {{
              showModal(false, 'Network error', String(e.message || e));
            }} finally {{
              setBusy(false);
            }}
          }});
        }}
      }})();

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

      const pageMatrix = document.getElementById('page-matrix');
      if (pageMatrix) {{
        pageMatrix.addEventListener('click', async (e) => {{
          const btn = e.target.closest('.dt-matrix-cell');
          if (!btn || btn.disabled || btn.classList.contains('dt-matrix-skip') || btn.classList.contains('dt-matrix-none')) return;
          const rx = btn.getAttribute('data-matrix-rx');
          const tx = btn.getAttribute('data-matrix-tx');
          if (!rx || tx === null || tx === undefined) return;
          const row = btn.closest('tr');
          const cells = row ? row.querySelectorAll('.dt-matrix-cell') : [];
          cells.forEach((el) => {{
            if (!el.classList.contains('dt-matrix-skip')) el.disabled = true;
            el.classList.add('dt-matrix-busy');
          }});
          try {{
            const r = await fetch(ctlUrl('/control/matrix_set?tx=' + encodeURIComponent(tx) + '&rx=' + encodeURIComponent(rx)));
            const j = await r.json().catch(() => ({{}}));
            if (!r.ok || !j.ok) {{
              showModal(false, 'Matrix', j.error || 'Request failed');
              return;
            }}
            if (row) {{
              const vt = j.video_tx;
              row.querySelectorAll('.dt-matrix-cell').forEach((el) => {{
                const t = el.getAttribute('data-matrix-tx');
                const on = (vt === null || vt === undefined) ? (t === 'NULL') : (vt === t);
                el.classList.toggle('dt-matrix-on', on);
              }});
            }}
          }} catch (err) {{
            showModal(false, 'Network error', String(err.message || err));
          }} finally {{
            cells.forEach((el) => {{
              el.classList.remove('dt-matrix-busy');
              if (!el.classList.contains('dt-matrix-skip') && !el.classList.contains('dt-matrix-none')) el.disabled = false;
            }});
          }}
        }});
      }}

      const homeOverviewCardEl = document.getElementById('homeOverviewCard');
      const devicesBodyEl = document.getElementById('devicesBody');
      const logsPreEl = document.getElementById('logsPre');
      const unknownCtlPreEl = document.getElementById('unknownCtlPre');
      const matrixCardEl = document.getElementById('matrixCard');
      let refreshInFlight = false;
      function shouldAutoRefresh() {{
        const p = window.location.pathname || '/';
        if (p === '/controls') return false;
        return true;
      }}
      async function refreshLiveSections() {{
        if (dtBusy || refreshInFlight || document.hidden) return;
        if (!shouldAutoRefresh()) return;
        refreshInFlight = true;
        try {{
          const r = await fetch(window.location.pathname + window.location.search, {{ cache: 'no-store' }});
          if (!r.ok) return;
          const t = await r.text();
          const doc = new DOMParser().parseFromString(t, 'text/html');
          const newHomeCard = doc.getElementById('homeOverviewCard');
          if (homeOverviewCardEl && newHomeCard) {{
            homeOverviewCardEl.innerHTML = newHomeCard.innerHTML;
          }}
          const newDevicesBody = doc.getElementById('devicesBody');
          const newLogsPre = doc.getElementById('logsPre');
          const newUnknownPre = doc.getElementById('unknownCtlPre');
          if (devicesBodyEl && newDevicesBody) {{
            devicesBodyEl.innerHTML = newDevicesBody.innerHTML;
            bindEndpointSkipButtons(devicesBodyEl);
          }}
          if (logsPreEl && newLogsPre) logsPreEl.textContent = newLogsPre.textContent || '';
          if (unknownCtlPreEl && newUnknownPre) unknownCtlPreEl.textContent = newUnknownPre.textContent || '';
          const newMatrixCard = doc.getElementById('matrixCard');
          if (matrixCardEl && newMatrixCard) {{
            matrixCardEl.innerHTML = newMatrixCard.innerHTML;
          }}
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
            writer.write(http_response("200 OK", "text/html; charset=utf-8", body))
            return

        writer.write(http_response("404 Not Found", "text/plain", b"not found"))
    finally:
        with contextlib.suppress(Exception):
            await writer.drain()
        writer.close()
        with contextlib.suppress(Exception):
            await writer.wait_closed()
