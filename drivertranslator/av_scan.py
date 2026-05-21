from __future__ import annotations

import asyncio
import ipaddress
import re
import socket
from typing import Any, Dict, List, Optional, Set, Tuple

from .matrix_amx import read_amx_status_fields_from_ip
from .models import Config, RuntimeSettings

# Concurrent probes per wave (matches previous semaphore limit).
_SCAN_CONCURRENCY = 48

# Set by scan_av_network; abort endpoint sets this event to stop between waves.
_scan_cancel_event: Optional[asyncio.Event] = None


def request_av_scan_cancel() -> None:
    ev = _scan_cancel_event
    if ev is not None:
        ev.set()


def _ipv4_hosts_from_spec(spec: str) -> List[str]:
    """
    Parse IPv4 range: CIDR (192.168.1.0/24), single host, or start-end (10.0.0.1-10.0.0.40).
    Skips network and broadcast addresses for subnet ranges.
    """
    s = spec.strip()
    if not s:
        raise ValueError("IP range is empty.")
    if "-" in s and "/" not in s:
        a, b = s.split("-", 1)
        a, b = a.strip(), b.strip()
        first = ipaddress.IPv4Address(a)
        last = ipaddress.IPv4Address(b)
        if int(first) > int(last):
            first, last = last, first
        n = int(last) - int(first) + 1
        if n > 4096:
            raise ValueError("Range too large (max 4096 addresses).")
        return [str(ipaddress.IPv4Address(i)) for i in range(int(first), int(last) + 1)]

    net = ipaddress.ip_network(s, strict=False)
    if not isinstance(net, ipaddress.IPv4Network):
        raise ValueError("Only IPv4 ranges are supported.")
    hosts = list(net.hosts()) if net.num_addresses > 1 else [net.network_address]
    if len(hosts) > 4096:
        raise ValueError("Range too large (max 4096 addresses).")
    return [str(h) for h in hosts]


def check_av_bind_address(bind_address: Optional[str]) -> Tuple[bool, str]:
    """
    Verify that amx.bind_address is assigned on this host (UDP bind test).
    If None, outbound AMX uses OS routing (no specific NIC check).
    """
    if not bind_address:
        return True, "No amx.bind_address configured; AMX traffic uses default routing."
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.bind((bind_address, 0))
        finally:
            s.close()
        return True, f"Address {bind_address} is usable on this host (local bind OK)."
    except OSError as e:
        return False, f"Cannot use amx.bind_address {bind_address!r} on this host: {e}"


def infer_default_scan_range(cfg: Config) -> str:
    """
    Default CIDR for the AV scan field: prefer amx.bind_address /24, else first endpoint /24.
    """
    if cfg.amx_bind_address:
        ip = ipaddress.IPv4Address(cfg.amx_bind_address)
        return f"{ip}/24"

    ips: List[ipaddress.IPv4Address] = []
    for t in cfg.tx_by_alias.values():
        if t.ip:
            ips.append(ipaddress.IPv4Address(t.ip))
    for r in cfg.rx_by_alias.values():
        for raw in (r.amx_decoder_ip, r.ip):
            if raw:
                ips.append(ipaddress.IPv4Address(raw))

    if not ips:
        if cfg.amx_bind_address:
            return f"{ipaddress.IPv4Address(cfg.amx_bind_address)}/24"
        return "192.168.10.0/24"

    first = ips[0]
    return f"{first}/24"


def classify_amx_kind(fields: Dict[str, str]) -> Optional[str]:
    """
    Distinguish encoder (tx) vs decoder (rx) from AMX getStatus fields.
    """
    hi = (fields.get("HDMIINPUT") or fields.get("DVIINPUT") or "").strip()
    ho = (fields.get("HDMIOFF") or "").strip()
    if hi and ho:
        hilo = hi.lower()
        if hilo in ("connected", "disconnected"):
            return "tx"
        holo = ho.lower()
        if holo in ("on", "off"):
            return "rx"
    if hi:
        return "tx"
    if ho:
        return "rx"
    return None


def _collect_existing_ips(cfg: Config) -> Tuple[Set[str], Set[str]]:
    tx_ips: Set[str] = set()
    rx_ips: Set[str] = set()
    for t in cfg.tx_by_alias.values():
        if t.ip:
            tx_ips.add(t.ip)
    for r in cfg.rx_by_alias.values():
        rx_ips.add(r.amx_decoder_ip)
        if r.ip:
            rx_ips.add(r.ip)
    return tx_ips, rx_ips


async def scan_av_network(
    *,
    cfg: Config,
    runtime: RuntimeSettings,
    range_spec: str,
) -> Dict[str, Any]:
    if cfg.amx_dry_run:
        return {"ok": False, "error": "Not available in dry-run mode."}

    bind_ok, bind_msg = check_av_bind_address(cfg.amx_bind_address)
    if not bind_ok:
        return {
            "ok": False,
            "error": bind_msg,
            "bind_check": {"ok": False, "message": bind_msg},
        }

    try:
        hosts = _ipv4_hosts_from_spec(range_spec)
    except ValueError as e:
        return {"ok": False, "error": str(e), "bind_check": {"ok": bind_ok, "message": bind_msg}}

    existing_tx, existing_rx = _collect_existing_ips(cfg)
    local_addr = (cfg.amx_bind_address, 0) if cfg.amx_bind_address else None
    timeout_ms = max(200, min(5000, int(runtime.amx_verify_timeout_ms)))
    port = cfg.amx_decoder_port

    found: List[Dict[str, Any]] = []
    global _scan_cancel_event
    cancel_event = asyncio.Event()
    _scan_cancel_event = cancel_event
    cancelled = False
    completed_probes = 0
    try:

        async def probe_one(ip: str) -> Optional[Dict[str, Any]]:
            if cancel_event.is_set():
                return None
            try:
                fields = await read_amx_status_fields_from_ip(
                    host=ip,
                    port=port,
                    connect_timeout_ms=cfg.amx_connect_timeout_ms,
                    timeout_ms=timeout_ms,
                    local_addr=local_addr,
                    expanded_log=runtime.expanded_log,
                )
            except Exception:
                return None
            if not fields:
                return None
            kind = classify_amx_kind(fields)
            if kind not in ("tx", "rx"):
                return None
            dup_note = ""
            is_dup = False
            if kind == "tx":
                is_dup = ip in existing_tx
                dup_note = _dup_note_tx(cfg, ip)
            else:
                is_dup = ip in existing_rx or ip in existing_tx
                dup_note = _dup_note_rx(cfg, ip)
            return {
                "ip": ip,
                "kind": kind,
                "fields": dict(sorted(fields.items())),
                "duplicate": is_dup,
                "duplicate_note": dup_note,
            }

        for i in range(0, len(hosts), _SCAN_CONCURRENCY):
            if cancel_event.is_set():
                cancelled = True
                break
            chunk = hosts[i : i + _SCAN_CONCURRENCY]
            results = await asyncio.gather(*[probe_one(h) for h in chunk])
            completed_probes += len(chunk)
            for r in results:
                if r is not None:
                    found.append(r)

        found.sort(key=lambda x: (ipaddress.IPv4Address(x["ip"]).packed, x["kind"]))

        out: Dict[str, Any] = {
            "ok": True,
            "bind_check": {"ok": bind_ok, "message": bind_msg},
            "range": range_spec.strip(),
            "scanned": len(hosts),
            "found": found,
        }
        if cancelled:
            out["cancelled"] = True
            out["completed_probes"] = completed_probes
        return out
    finally:
        _scan_cancel_event = None


def _dup_note_tx(cfg: Config, ip: str) -> str:
    for t in cfg.tx_by_alias.values():
        if t.ip == ip:
            return f"Already in project as TX {t.alias}"
    return ""


def _dup_note_rx(cfg: Config, ip: str) -> str:
    for r in cfg.rx_by_alias.values():
        if r.amx_decoder_ip == ip or r.ip == ip:
            return f"Already in project as RX {r.alias}"
    return ""


def next_suggested_aliases(cfg: Config) -> Dict[str, str]:
    """Suggest next INn-BOXn / OUTn-TVn style aliases from current endpoints."""
    max_in = 0
    max_out = 0
    for t in cfg.tx_by_alias.values():
        m = re.match(r"^IN(\d+)-", t.alias)
        if m:
            max_in = max(max_in, int(m.group(1)))
    for r in cfg.rx_by_alias.values():
        m = re.match(r"^OUT(\d+)-", r.alias)
        if m:
            max_out = max(max_out, int(m.group(1)))
    n_in = max_in + 1
    n_out = max_out + 1
    return {
        "next_tx_alias": f"IN{n_in}-BOX{n_in}",
        "next_rx_alias": f"OUT{n_out}-TV{n_out}",
    }


