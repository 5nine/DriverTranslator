from __future__ import annotations

import contextlib
from typing import Any, Dict, List

from .models import Config
from .networking import open_connection


async def amx_self_test(*, cfg: Config, amx: Any) -> Dict[str, Any]:
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
            _r, w = await open_connection(
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
