from __future__ import annotations

from typing import Dict, List, Optional, Tuple

from .models import Config, ControllerState, Rx, Tx


def lookup_tx(cfg: Config, token: str) -> Optional[Tx]:
    t = (token or "").strip()
    return cfg.tx_by_alias.get(t) or cfg.tx_by_hostname.get(t)


def lookup_rx(cfg: Config, token: str) -> Optional[Rx]:
    t = (token or "").strip()
    return cfg.rx_by_alias.get(t) or cfg.rx_by_hostname.get(t)


def tx_alias_from_amx_stream(cfg: Config, stream_value: Optional[str]) -> Optional[str]:
    s = (stream_value or "").strip()
    if not s:
        return None
    for tx in cfg.tx_by_alias.values():
        if str(tx.amx_stream) == s:
            return tx.alias
    return None


def all_endpoint_aliases(cfg: Config) -> List[str]:
    return list(cfg.tx_by_alias.keys()) + list(cfg.rx_by_alias.keys())


def emulated_multicast_ips(*, stream_id: int) -> Tuple[str, str]:
    """Stable fake multicast addresses for TX status (100/200-series style API)."""
    s = max(0, int(stream_id))
    v = 16 + (s % 200)
    a = 40 + (s * 7 % 200)
    return (f"224.{v}.{a}.{200 + (s % 55)}", f"224.{v + 32}.{a}.{200 + (s % 55)}")


def device_status_tx_dict(tx: Tx) -> Dict[str, str]:
    vid, aud = emulated_multicast_ips(stream_id=tx.amx_stream)
    return {
        "aliasname": tx.alias,
        "audio stream ip address": aud,
        "encoding enable": "true",
        "hdmi in active": "true",
        "hdmi in frame rate": "60",
        "line out audio enable": "false",
        "name": tx.hostname,
        "resolution": "1920x1080",
        "stream frame rate": "60",
        "stream resolution": "1920x1080",
        "video stream ip address": vid,
    }


def format_tx_signal(fields: Dict[str, str]) -> Tuple[str, str]:
    hdmi_in = (fields.get("HDMIINPUT") or "").strip().lower()
    input_res = (fields.get("INPUTRES") or "").strip()
    if hdmi_in == "connected":
        if input_res:
            return (f"HDMI IN: {input_res}", "ok")
        return ("HDMI IN: CONNECTED", "ok")
    if hdmi_in == "disconnected":
        return ("HDMI IN: DISCONNECTED", "bad")
    if input_res:
        return (f"IN RES: {input_res}", "")
    return ("UNKNOWN", "")


def device_status_rx_dict(rx: Rx, state: ControllerState) -> Dict[str, str]:
    online = bool(state.rx_online.get(rx.alias, True))
    hdmi_enabled = state.rx_hdmi_output.get(rx.alias)
    routed_tx = state.video.get(rx.alias)
    hdmi_out_active = "true" if hdmi_enabled is not False else "false"
    if online and routed_tx:
        return {
            "aliasname": rx.alias,
            "audio bitrate": "3072000",
            "audio input format": "lpcm",
            "hdcp status": "hdcp22",
            "hdmi out active": hdmi_out_active,
            "hdmi out audio enable": "true",
            "hdmi out frame rate": "60",
            "hdmi out resolution": "1920x1080",
            "line out audio enable": "true",
            "name": rx.hostname,
            "stream error count": "0",
            "stream frame rate": "60",
            "stream resolution": "1920x1080",
        }
    if online:
        return {
            "aliasname": rx.alias,
            "audio bitrate": "0",
            "audio input format": "lpcm",
            "hdcp status": "none",
            "hdmi out active": hdmi_out_active,
            "hdmi out audio enable": "false",
            "hdmi out frame rate": "0",
            "hdmi out resolution": "unknown",
            "line out audio enable": "false",
            "name": rx.hostname,
            "stream error count": "0",
            "stream frame rate": "0",
            "stream resolution": "unknown",
        }
    return {
        "aliasname": rx.alias,
        "audio bitrate": "0",
        "audio input format": "unknown",
        "hdcp status": "none",
        "hdmi out active": "false",
        "hdmi out audio enable": "false",
        "hdmi out frame rate": "0",
        "hdmi out resolution": "unknown",
        "line out audio enable": "false",
        "name": rx.hostname,
        "stream error count": "65535",
        "stream frame rate": "0",
        "stream resolution": "unknown",
    }


def format_matrix_info(heading: str, mapping: Dict[str, Optional[str]], rx_aliases: List[str]) -> List[str]:
    lines: List[str] = [f"{heading} information:"]
    for rx in rx_aliases:
        tx = mapping.get(rx)
        lines.append(f"{(tx if tx is not None else 'NULL')} {rx}")
    return lines


def as_success(line: str) -> str:
    # Some WyreStorm API commands explicitly append success|failure, others just mirror.
    # Returning success for state-mutating commands keeps RTI drivers happy.
    if line.endswith(" success") or line.endswith(" failure"):
        return line
    return f"{line} success"
