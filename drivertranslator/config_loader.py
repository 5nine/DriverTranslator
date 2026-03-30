from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import List, Set

from .models import Config, NhdCtlIdentity, Rx, Tx
from .utils import as_bool, as_int, bind_addr, clamp_int, opt_str

LOG = logging.getLogger("drivertranslator")


def load_config(path: str) -> Config:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))

    nhd_raw = raw.get("nhd_ctl", {})
    ver = nhd_raw.get("version", {})
    ip1 = nhd_raw.get("ipsetting", {})
    ip2 = nhd_raw.get("ipsetting2", {})

    nhd = NhdCtlIdentity(
        api=str(ver.get("api", "1.21")),
        web=str(ver.get("web", "8.3.1")),
        core=str(ver.get("core", "8.3.8")),
        ipsetting={
            "ip4addr": str(ip1.get("ip4addr", "169.254.1.1")),
            "netmask": str(ip1.get("netmask", "255.255.0.0")),
            "gateway": str(ip1.get("gateway", "169.254.1.254")),
        },
        ipsetting2={
            "ip4addr": str(ip2.get("ip4addr", "192.168.11.243")),
            "netmask": str(ip2.get("netmask", "255.255.255.0")),
            "gateway": str(ip2.get("gateway", "192.168.11.1")),
        },
    )

    endpoints = raw.get("endpoints", {})
    tx_list = endpoints.get("tx", [])
    rx_list = endpoints.get("rx", [])

    txs: List[Tx] = []
    tx_skipped_aliases: Set[str] = set()
    for t in tx_list:
        alias = str(t["alias"]).strip(" \t\r\n\u00a0")
        if as_bool(t.get("skip"), default=False):
            tx_skipped_aliases.add(alias)
        hostname = str(t.get("hostname") or f"NHD-TX-{alias}").strip(" \t\r\n\u00a0")
        ip = t.get("ip")
        ip_s = str(ip).strip() if ip is not None and str(ip).strip() else None
        amx_stream = as_int(t.get("amx_stream"), default=0)
        txs.append(Tx(alias=alias, hostname=hostname, ip=ip_s, amx_stream=amx_stream))

    rxs: List[Rx] = []
    rx_skipped_aliases: Set[str] = set()
    for r in rx_list:
        alias = str(r["alias"]).strip(" \t\r\n\u00a0")
        if as_bool(r.get("skip"), default=False):
            rx_skipped_aliases.add(alias)
        hostname = str(r.get("hostname") or f"NHD-RX-{alias}").strip(" \t\r\n\u00a0")
        ip = r.get("ip")
        ip_s = str(ip).strip() if ip is not None and str(ip).strip() else None
        amx_decoder_ip = str(r["amx_decoder_ip"]).strip(" \t\r\n\u00a0")
        rxs.append(Rx(alias=alias, hostname=hostname, ip=ip_s, amx_decoder_ip=amx_decoder_ip))

    amx = raw.get("amx", {})
    server = raw.get("server", {})
    http_status = raw.get("http_status", {})
    rti_control = raw.get("rti_control", {})
    unknown_ctl = raw.get("unknown_ctl") if isinstance(raw.get("unknown_ctl"), dict) else {}
    uc_pp = unknown_ctl.get("persist_path")
    uc_path_s = str(uc_pp).strip() if uc_pp is not None and str(uc_pp).strip() else None

    tx_by_alias = {t.alias: t for t in txs}
    tx_by_hostname = {t.hostname: t for t in txs}
    rx_by_alias = {r.alias: r for r in rxs}
    rx_by_hostname = {r.hostname: r for r in rxs}

    offline_decoders: List[str] = []
    od = amx.get("dry_run_offline_decoders", [])
    if isinstance(od, str):
        offline_decoders = [x.strip() for x in od.split(",") if x.strip()]
    elif isinstance(od, list):
        offline_decoders = [str(x).strip() for x in od if str(x).strip()]

    return Config(
        nhd=nhd,
        tx_by_alias=tx_by_alias,
        tx_by_hostname=tx_by_hostname,
        rx_by_alias=rx_by_alias,
        rx_by_hostname=rx_by_hostname,
        tx_skipped_aliases=tx_skipped_aliases,
        rx_skipped_aliases=rx_skipped_aliases,
        amx_decoder_port=as_int(amx.get("decoder_port"), default=50002),
        amx_connect_timeout_ms=as_int(amx.get("connect_timeout_ms"), default=1000),
        amx_command_timeout_ms=as_int(amx.get("command_timeout_ms"), default=1500),
        expanded_log=as_bool(server.get("expanded_log"), default=False),
        amx_dry_run=bool(amx.get("dry_run", False)),
        amx_persistent=bool(amx.get("persistent", False)),
        amx_keepalive_seconds=as_int(amx.get("keepalive_seconds"), default=30),
        amx_bind_address=bind_addr(amx.get("bind_address")),  # AVoIP NIC for outbound AMX
        amx_dry_run_offline_decoders=offline_decoders,
        amx_verify_after_set=as_bool(amx.get("verify_after_set"), default=True),
        amx_verify_timeout_ms=clamp_int(amx.get("verify_timeout_ms"), default=800, min_v=100, max_v=5000),
        amx_set_queue_limit=clamp_int(amx.get("set_queue_limit"), default=1, min_v=1, max_v=20),
        amx_self_test_on_start=as_bool(amx.get("self_test_on_start"), default=True),
        amx_set_retry_attempts=clamp_int(amx.get("set_retry_attempts"), default=3, min_v=1, max_v=10),
        amx_set_retry_backoff_initial_ms=clamp_int(amx.get("set_retry_backoff_initial_ms"), default=200, min_v=0, max_v=5000),
        amx_set_retry_backoff_max_ms=clamp_int(amx.get("set_retry_backoff_max_ms"), default=1200, min_v=0, max_v=10000),
        http_status_enabled=as_bool(http_status.get("enabled"), default=True),
        http_status_bind=str(http_status.get("bind", "0.0.0.0")).strip() or "0.0.0.0",
        http_status_port=as_int(http_status.get("port"), default=8080),
        http_status_log_lines=as_int(http_status.get("log_lines"), default=200),
        http_status_control_token=opt_str(http_status.get("control_token")),
        http_status_password=str(http_status.get("password", "1234")),
        rti_control_enabled=as_bool(rti_control.get("enabled"), default=False),
        rti_control_bind_address=bind_addr(rti_control.get("bind_address")),
        rti_control_port=as_int(rti_control.get("port"), default=0),
        rti_control_reboot_command=str(rti_control.get("reboot_command", "reboot")).strip() or "reboot",
        unknown_ctl_enabled=as_bool(unknown_ctl.get("enabled"), default=True),
        unknown_ctl_persist_path=uc_path_s,
    )


def validate_config(cfg: Config) -> None:
    errors: List[str] = []
    warnings: List[str] = []

    if not cfg.tx_by_alias:
        errors.append("No TX endpoints configured.")
    if not cfg.rx_by_alias:
        errors.append("No RX endpoints configured.")

    # Aliases and streams
    for tx in cfg.tx_by_alias.values():
        if not tx.alias.upper().startswith("IN"):
            warnings.append(f"TX alias does not start with IN: {tx.alias}")
        if tx.amx_stream <= 0:
            errors.append(f"TX has invalid amx_stream (must be > 0): {tx.alias} -> {tx.amx_stream}")

    for rx in cfg.rx_by_alias.values():
        if not rx.alias.upper().startswith("OUT"):
            warnings.append(f"RX alias does not start with OUT: {rx.alias}")
        if not rx.amx_decoder_ip:
            errors.append(f"RX missing amx_decoder_ip: {rx.alias}")

    if cfg.amx_dry_run and cfg.amx_persistent:
        errors.append("Config invalid: amx.dry_run=true and amx.persistent=true cannot both be enabled.")

    if cfg.http_status_port <= 0 or cfg.http_status_port > 65535:
        errors.append(f"Invalid http_status.port: {cfg.http_status_port}")

    if warnings:
        for w in warnings:
            LOG.warning("Config warning: %s", w)

    if errors:
        raise ValueError("Config validation failed:\n- " + "\n- ".join(errors))
