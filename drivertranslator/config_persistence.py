from __future__ import annotations

import ipaddress
import json
import re
import threading
from pathlib import Path
from typing import Any, Dict, List, Tuple

from .utils import as_bool

# Shared lock for all JSON config writes (HTTP control + endpoint editor).
config_write_lock = threading.Lock()


def persist_runtime_setting_to_config(*, config_path: str, key: str, value: Any) -> None:
    mapping: Dict[str, Tuple[str, str]] = {
        "amx_dry_run": ("amx", "dry_run"),
        "amx_persistent": ("amx", "persistent"),
        "amx_verify_after_set": ("amx", "verify_after_set"),
        "amx_verify_timeout_ms": ("amx", "verify_timeout_ms"),
        "expanded_log": ("server", "expanded_log"),
    }
    target = mapping.get(key)
    if target is None:
        return
    section, leaf = target
    path = Path(config_path).expanduser().resolve()
    with config_write_lock:
        raw = json.loads(path.read_text(encoding="utf-8"))
        obj = raw.get(section)
        if not isinstance(obj, dict):
            obj = {}
            raw[section] = obj
        obj[leaf] = value
        path.write_text(json.dumps(raw, indent=2) + "\n", encoding="utf-8")


def generate_endpoints_from_size(
    *,
    tx_count: int,
    rx_count: int,
    tx_start_ip: str,
    rx_start_ip: str,
) -> Dict[str, List[Dict[str, Any]]]:
    if tx_count <= 0 or rx_count <= 0:
        raise ValueError("TX and RX counts must be greater than zero.")
    # Keep a practical upper bound for a web-entered size.
    if tx_count > 512 or rx_count > 512:
        raise ValueError("TX and RX counts must be between 1 and 512.")

    tx_start = ipaddress.IPv4Address(tx_start_ip.strip())
    rx_start = ipaddress.IPv4Address(rx_start_ip.strip())

    tx: List[Dict[str, Any]] = []
    for i in range(tx_count):
        n = i + 1
        ip = str(tx_start + i)
        tx.append(
            {
                "alias": f"IN{n}-BOX{n}",
                "hostname": f"NHD-120-TX-{n:012d}",
                "ip": ip,
                "amx_stream": n,
            }
        )

    rx: List[Dict[str, Any]] = []
    for i in range(rx_count):
        n = i + 1
        ip = str(rx_start + i)
        rx.append(
            {
                "alias": f"OUT{n}-TV{n}",
                "hostname": f"NHD-120-RX-{(100 + n):012d}",
                "ip": ip,
                "amx_decoder_ip": ip,
            }
        )
    return {"tx": tx, "rx": rx}


def persist_endpoints_to_config(
    *,
    config_path: str,
    tx_count: int,
    rx_count: int,
    tx_start_ip: str,
    rx_start_ip: str,
) -> Dict[str, Any]:
    endpoints = generate_endpoints_from_size(
        tx_count=tx_count,
        rx_count=rx_count,
        tx_start_ip=tx_start_ip,
        rx_start_ip=rx_start_ip,
    )
    path = Path(config_path).expanduser().resolve()
    with config_write_lock:
        raw = json.loads(path.read_text(encoding="utf-8"))
        raw["endpoints"] = endpoints
        path.write_text(json.dumps(raw, indent=2) + "\n", encoding="utf-8")
    return {
        "ok": True,
        "tx_count": tx_count,
        "rx_count": rx_count,
        "tx_start_ip": str(ipaddress.IPv4Address(tx_start_ip.strip())),
        "rx_start_ip": str(ipaddress.IPv4Address(rx_start_ip.strip())),
        "restart_required": True,
    }


def _max_in_out_numbers(tx_rows: List[Dict[str, Any]], rx_rows: List[Dict[str, Any]]) -> Tuple[int, int]:
    max_in = 0
    max_out = 0
    for row in tx_rows:
        if not isinstance(row, dict):
            continue
        m = re.match(r"^IN(\d+)-", str(row.get("alias", "")))
        if m:
            max_in = max(max_in, int(m.group(1)))
    for row in rx_rows:
        if not isinstance(row, dict):
            continue
        m = re.match(r"^OUT(\d+)-", str(row.get("alias", "")))
        if m:
            max_out = max(max_out, int(m.group(1)))
    return max_in, max_out


def _max_amx_stream(tx_rows: List[Dict[str, Any]]) -> int:
    m = 0
    for row in tx_rows:
        if not isinstance(row, dict):
            continue
        try:
            s = int(row.get("amx_stream", 0))
            m = max(m, s)
        except Exception:
            pass
    return m


def append_av_endpoints_to_config(
    *,
    config_path: str,
    tx_ips: List[str],
    rx_ips: List[str],
) -> Dict[str, Any]:
    """
    Append TX/RX rows for AV scan adds. Skips IPs already present on the opposite role or duplicate.
    Uses INn-BOXn / OUTn-TVn naming and sequential amx_stream for new TX.
    """
    path = Path(config_path).expanduser().resolve()
    with config_write_lock:
        raw = json.loads(path.read_text(encoding="utf-8"))
        endpoints = raw.get("endpoints")
        if not isinstance(endpoints, dict):
            raise ValueError("config missing endpoints section.")
        tx_rows = endpoints.get("tx")
        rx_rows = endpoints.get("rx")
        if not isinstance(tx_rows, list):
            tx_rows = []
            endpoints["tx"] = tx_rows
        if not isinstance(rx_rows, list):
            rx_rows = []
            endpoints["rx"] = rx_rows

        tx_ip_set = {
            str(ipaddress.IPv4Address(str(r.get("ip", "")).strip()))
            for r in tx_rows
            if isinstance(r, dict) and str(r.get("ip", "")).strip()
        }
        rx_dec_set = {
            str(ipaddress.IPv4Address(str(r.get("amx_decoder_ip", "")).strip()))
            for r in rx_rows
            if isinstance(r, dict) and str(r.get("amx_decoder_ip", "")).strip()
        }
        rx_ip_set = {
            str(ipaddress.IPv4Address(str(r.get("ip", "")).strip()))
            for r in rx_rows
            if isinstance(r, dict) and str(r.get("ip", "")).strip()
        }

        added_tx: List[str] = []
        added_rx: List[str] = []
        skipped: List[str] = []

        max_in, max_out = _max_in_out_numbers(tx_rows, rx_rows)
        next_stream = _max_amx_stream(tx_rows)

        def norm_ip(s: str) -> str:
            return str(ipaddress.IPv4Address(s.strip()))

        for raw_ip in tx_ips:
            ip = norm_ip(raw_ip)
            if ip in tx_ip_set:
                skipped.append(f"tx {ip} (already a TX)")
                continue
            if ip in rx_dec_set or ip in rx_ip_set:
                skipped.append(f"tx {ip} (already an RX; remove RX first)")
                continue
            max_in += 1
            n = max_in
            next_stream += 1
            tx_rows.append(
                {
                    "alias": f"IN{n}-BOX{n}",
                    "hostname": f"NHD-120-TX-{n:012d}",
                    "ip": ip,
                    "amx_stream": next_stream,
                }
            )
            tx_ip_set.add(ip)
            added_tx.append(ip)

        for raw_ip in rx_ips:
            ip = norm_ip(raw_ip)
            if ip in rx_dec_set:
                skipped.append(f"rx {ip} (already an RX decoder IP)")
                continue
            if ip in tx_ip_set:
                skipped.append(f"rx {ip} (already a TX; remove TX first)")
                continue
            max_out += 1
            n = max_out
            rx_rows.append(
                {
                    "alias": f"OUT{n}-TV{n}",
                    "hostname": f"NHD-120-RX-{(100 + n):012d}",
                    "ip": ip,
                    "amx_decoder_ip": ip,
                }
            )
            rx_dec_set.add(ip)
            rx_ip_set.add(ip)
            added_rx.append(ip)

        path.write_text(json.dumps(raw, indent=2) + "\n", encoding="utf-8")

    return {
        "ok": True,
        "added_tx": added_tx,
        "added_rx": added_rx,
        "skipped": skipped,
        "restart_required": True,
    }


def load_endpoint_inventory(*, config_path: str) -> Dict[str, List[Dict[str, Any]]]:
    path = Path(config_path).expanduser().resolve()
    raw = json.loads(path.read_text(encoding="utf-8"))
    endpoints = raw.get("endpoints", {}) if isinstance(raw, dict) else {}
    out: Dict[str, List[Dict[str, Any]]] = {"tx": [], "rx": []}
    for kind in ("tx", "rx"):
        rows = endpoints.get(kind, [])
        if not isinstance(rows, list):
            continue
        for row in rows:
            if not isinstance(row, dict):
                continue
            alias = str(row.get("alias", "")).strip()
            if not alias:
                continue
            # Keep skip parsing consistent with runtime config loading.
            out[kind].append({"alias": alias, "skip": as_bool(row.get("skip"), default=False)})
    return out


def persist_endpoint_skip_to_config(*, config_path: str, kind: str, alias: str, skip: bool) -> Dict[str, Any]:
    if kind not in ("tx", "rx"):
        raise ValueError("kind must be 'tx' or 'rx'.")
    alias_s = alias.strip()
    if not alias_s:
        raise ValueError("alias is required.")
    path = Path(config_path).expanduser().resolve()
    with config_write_lock:
        raw = json.loads(path.read_text(encoding="utf-8"))
        endpoints = raw.get("endpoints")
        if not isinstance(endpoints, dict):
            raise ValueError("config missing endpoints section.")
        rows = endpoints.get(kind)
        if not isinstance(rows, list):
            raise ValueError(f"config endpoints.{kind} is not a list.")
        found = False
        for row in rows:
            if not isinstance(row, dict):
                continue
            if str(row.get("alias", "")).strip() == alias_s:
                row["skip"] = bool(skip)
                found = True
                break
        if not found:
            raise ValueError(f"{kind.upper()} alias not found: {alias_s}")
        path.write_text(json.dumps(raw, indent=2) + "\n", encoding="utf-8")
    return {
        "ok": True,
        "kind": kind,
        "alias": alias_s,
        "skip": bool(skip),
        "restart_required": True,
    }


def ctl_json(v: Any) -> str:
    s = json.dumps(v, separators=(", ", " : "), ensure_ascii=False)
    s = s.replace("{", "{ ").replace("}", " }").replace("[", "[ ").replace("]", " ]")
    return s
