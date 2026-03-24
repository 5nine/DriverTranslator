from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import logging
import secrets
import socket
import subprocess
import time
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
from .log_ring import RingBufferLogHandler as _RingBufferLogHandler
from .problem_reporter import LocalProblemReporter
from .system_control import do_reboot as _do_reboot, do_service_restart as _do_service_restart
from .config_persistence import (
    ctl_json as _ctl_json,
    generate_endpoints_from_size as _generate_endpoints_from_size,
)
from .http_status import (
    TX_STATUS_POLL_INTERVAL_SECONDS as _TX_STATUS_POLL_INTERVAL_SECONDS,
    handle_http_client as _handle_http_client,
)

# Quick index (major sections in this file):
# - Unknown-command tracking: unknown_ctl.py
# - Config loading/validation: config_loader.py; JSON edits: config_persistence.py
# - HTTP response/snapshot helpers: http_helpers.py; status/control handler: http_status.py
# - TCP/AMX: networking.py, amx_protocol.py, amx_client.py
# - Log ring: log_ring.py; HTTP UI session tokens: http_ui_session.py
# - LocalProblemReporter: problem_reporter.py; reboot/restart: system_control.py
# - Shared controller/runtime state (models.py)
# - RTI/NHD-CTL protocol helpers and command handlers
# - Server bootstrap + process entrypoint
LOG = logging.getLogger("drivertranslator")


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

    # Section 13.2 â€” device real-time status (100/110/140/200-tier JSON shape, API v6.6 / Appendix-style).
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
                # matrix get [<RX...>]  â€” primary all-media assignments (Â§13.3)
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
                amx_self_test=_amx_self_test,
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

