from __future__ import annotations

import asyncio
import logging
import time

from .models import Config
from .system_control import do_reboot

LOG = logging.getLogger("drivertranslator")


class RtiControlUdp(asyncio.DatagramProtocol):
    """UDP listener for optional RTI control commands (e.g. host reboot)."""

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
        asyncio.create_task(do_reboot(reason=f"rti_udp:{addr}"))
