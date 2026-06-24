from __future__ import annotations

import asyncio
import logging
import subprocess
import sys

LOG = logging.getLogger("drivertranslator")

# Programmatic reboot/service-restart relies on systemd (Linux). On other
# platforms (notably Windows) there is no equivalent we can safely drive, so
# the operator must perform these actions manually. can_self_manage() lets the
# control surfaces (HTTP page) tell the user that up front instead of silently
# failing after the fact.
SELF_MANAGED = sys.platform.startswith("linux")

# Shown to the operator when an action cannot be performed automatically.
MANUAL_REBOOT_MESSAGE = (
    "Automatic reboot is not supported on this host (only Linux/systemd). "
    "Please reboot the machine manually."
)
MANUAL_RESTART_MESSAGE = (
    "Automatic service restart is not supported on this host (only Linux/systemd). "
    "Please restart the DriverTranslator process/service manually."
)


def can_self_manage() -> bool:
    """True when this host can reboot/restart itself programmatically (systemd Linux)."""
    return SELF_MANAGED


async def do_reboot(*, reason: str) -> bool:
    """Reboot the host.

    Returns True if a reboot was initiated, False if unsupported on this
    platform (operator must reboot manually).
    """
    if not SELF_MANAGED:
        LOG.warning(
            "REBOOT requested (%s) but not supported on platform %r; operator must reboot manually.",
            reason,
            sys.platform,
        )
        return False
    LOG.error("REBOOT requested: %s", reason)
    await asyncio.sleep(1.0)
    try:
        subprocess.Popen(["/usr/bin/systemctl", "reboot"])
    except FileNotFoundError:
        subprocess.Popen(["systemctl", "reboot"])
    return True


async def do_service_restart(*, reason: str) -> bool:
    """Restart the DriverTranslator service.

    Returns True if a restart was initiated, False if unsupported on this
    platform (operator must restart manually).
    """
    if not SELF_MANAGED:
        LOG.warning(
            "SERVICE RESTART requested (%s) but not supported on platform %r; operator must restart manually.",
            reason,
            sys.platform,
        )
        return False
    LOG.warning("SERVICE RESTART requested: %s", reason)
    await asyncio.sleep(0.5)
    cmds = [
        ["/usr/bin/systemctl", "restart", "drivertranslator"],
        ["systemctl", "restart", "drivertranslator"],
    ]
    for cmd in cmds:
        try:
            subprocess.Popen(cmd)
            return True
        except FileNotFoundError:
            continue
        except Exception:
            LOG.exception("Failed to execute service restart command: %r", cmd)
            return False
    LOG.error("Unable to restart service: systemctl not found")
    return False
