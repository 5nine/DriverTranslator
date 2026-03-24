from __future__ import annotations

import logging
from typing import Dict, Optional

LOG = logging.getLogger("drivertranslator")


def parse_amx_status(data: bytes) -> Dict[str, str]:
    """
    AMX getStatus responses are \\r-delimited key:value lines (per AMX direct control API).
    We parse a best-effort mapping for fields we care about (e.g. STREAM).
    """
    try:
        text = data.decode("utf-8", errors="replace")
    except Exception:
        return {}
    out: Dict[str, str] = {}
    for raw in text.replace("\n", "\r").split("\r"):
        line = raw.strip()
        if not line:
            continue
        if ":" in line:
            k, v = line.split(":", 1)
            out[k.strip().upper()] = v.strip()
    return out


def log_amx_inbound(*, enabled: bool, decoder_ip: str, decoder_port: int, data: bytes) -> None:
    if not enabled:
        return
    if not data:
        LOG.info("AMX <- %s:%d <empty>", decoder_ip, decoder_port)
        return
    LOG.info("AMX <- %s:%d %r", decoder_ip, decoder_port, data)


def hdmi_enabled_from_status_fields(fields: Dict[str, str]) -> Optional[bool]:
    hdmi_off = (fields.get("HDMIOFF") or "").strip().lower()
    if hdmi_off == "on":
        return False
    if hdmi_off == "off":
        return True
    return None
