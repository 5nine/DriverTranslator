from __future__ import annotations

import asyncio
import logging
import subprocess

LOG = logging.getLogger("drivertranslator")


async def do_reboot(*, reason: str) -> None:
    LOG.error("REBOOT requested: %s", reason)
    await asyncio.sleep(1.0)
    try:
        subprocess.Popen(["/usr/bin/systemctl", "reboot"])
    except FileNotFoundError:
        subprocess.Popen(["systemctl", "reboot"])


async def do_service_restart(*, reason: str) -> None:
    LOG.warning("SERVICE RESTART requested: %s", reason)
    await asyncio.sleep(0.5)
    cmds = [
        ["/usr/bin/systemctl", "restart", "drivertranslator"],
        ["systemctl", "restart", "drivertranslator"],
    ]
    for cmd in cmds:
        try:
            subprocess.Popen(cmd)
            return
        except FileNotFoundError:
            continue
        except Exception:
            LOG.exception("Failed to execute service restart command: %r", cmd)
            return
    LOG.error("Unable to restart service: systemctl not found")
