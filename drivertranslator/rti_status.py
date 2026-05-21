from __future__ import annotations

import asyncio
import logging
from typing import Dict, List, Optional, Tuple

# (status, hdmi_state, tx_state) for RTI Two Way Strings TX lines (boolean-friendly key=value)
TxRtiFields = Tuple[str, str, str]
# (status, hdmi_out, hdmi_link, rx_state) — used internally for RX fault summary only
RxRtiFields = Tuple[str, str, str, str]

# Cap fault list length so DTRXSUMMARY stays readable on panel string variables.
_RX_SUMMARY_MAX_FAULTS = 12

from .models import Config, ControllerState, HealthState, RuntimeSettings
from .protocol_helpers import classify_tx_input
from .rti_telemetry import RtiTwoWayTransport
from .utils import rx_alias_sort_key, tx_alias_sort_key

LOG = logging.getLogger("drivertranslator")


def _tri_token(v: Optional[bool], *, on: str, off: str, unknown: str = "UNKNOWN") -> str:
    if v is True:
        return on
    if v is False:
        return off
    return unknown


def build_rx_rti_fields(*, state: ControllerState, rx_alias: str) -> RxRtiFields:
    """
    RTI-facing RX telemetry (per receiver).

    Uses the same polled state as the web Status page (from AMX getStatus / command
    responses): DVIOFF or HDMIOFF → hdmi-out; DVISTATUS or HDMISTATUS → hdmi-link.

    - status: ok | error  (ok when decoder online, HDMI out on, and sink link connected)
    - hdmi-out: on | off | unknown | null
    - hdmi-link: connected | disconnected | unknown | null
    - rx-state: connected | disconnected  (decoder TCP reachable)
    """
    rx_state = "connected" if state.rx_online.get(rx_alias, True) else "disconnected"
    if rx_state == "disconnected":
        return ("error", "null", "null", "disconnected")

    hdmi_out = _tri_token(state.rx_hdmi_output.get(rx_alias), on="on", off="off")
    hdmi_link = _tri_token(
        state.rx_hdmi_link.get(rx_alias),
        on="connected",
        off="disconnected",
    )
    status = "ok" if hdmi_out == "on" and hdmi_link == "connected" else "error"
    return (status, hdmi_out, hdmi_link, rx_state)


def _rx_fault_detail(*, state: ControllerState, rx_alias: str) -> Optional[str]:
    """Human-readable fault for one RX, or None if ok."""
    status, hdmi_out, hdmi_link, rx_state = build_rx_rti_fields(state=state, rx_alias=rx_alias)
    if status == "ok":
        return None
    if rx_state == "disconnected":
        return "offline"
    parts: List[str] = []
    if hdmi_out == "off":
        parts.append("HDMI output off")
    elif hdmi_out == "unknown":
        parts.append("HDMI output unknown")
    if hdmi_link == "disconnected":
        parts.append("TV disconnected")
    elif hdmi_link == "unknown":
        parts.append("TV link unknown")
    return ", ".join(parts) if parts else "fault"


def format_dt_rx_summary_line(*, cfg: Config, state: ControllerState) -> str:
    """
    Single RTI string variable: entire message after DTRXSUMMARY is shown to the user.

    No per-RX boolean mapping — only a readable summary when any RX has a problem.
    """
    rx_active = [a for a in cfg.rx_by_alias.keys() if a not in cfg.rx_skipped_aliases]
    faults: List[str] = []
    for rx_alias in sorted(rx_active, key=rx_alias_sort_key):
        detail = _rx_fault_detail(state=state, rx_alias=rx_alias)
        if detail:
            faults.append(f"{rx_alias} ({detail})")
    if not faults:
        n = len(rx_active)
        msg = f"All {n} RX OK"
    else:
        shown = faults[:_RX_SUMMARY_MAX_FAULTS]
        msg = f"{len(faults)} RX fault(s): " + "; ".join(shown)
        if len(faults) > _RX_SUMMARY_MAX_FAULTS:
            msg += f"; +{len(faults) - _RX_SUMMARY_MAX_FAULTS} more"
    return f"DTRXSUMMARY {msg}"


def build_tx_rti_fields(*, state: ControllerState, tx_alias: str) -> TxRtiFields:
    """
    RTI-facing TX telemetry (per transmitter).

    - status: ok | error  (error if TX offline or HDMI not ok)
    - hdmi-state: connected | disconnected | no signal | null (null when TX offline)
    - tx-state: connected | disconnected  (AMX TCP reachable)
    """
    tx_state = "connected" if state.tx_online.get(tx_alias, False) else "disconnected"
    if tx_state == "disconnected":
        return ("error", "null", "disconnected")

    inp = classify_tx_input(state.tx_status_fields.get(tx_alias, {}))
    if inp == "OK":
        hdmi_state = "connected"
    elif inp == "NO_SIGNAL":
        hdmi_state = "no signal"
    else:
        # DISCONNECTED, UNKNOWN, or other → disconnected
        hdmi_state = "disconnected"

    status = "ok" if hdmi_state == "connected" else "error"
    return (status, hdmi_state, tx_state)


def format_dt_tx_line(*, state: ControllerState, tx_alias: str) -> str:
    status, hdmi_state, tx_state = build_tx_rti_fields(state=state, tx_alias=tx_alias)
    return (
        f"DTTX {tx_alias} status={status} hdmi-state={hdmi_state} tx-state={tx_state}"
    )


def format_dt_status_summary(*, cfg: Config, state: ControllerState, health: HealthState) -> str:
    tx_active = [a for a in cfg.tx_by_alias.keys() if a not in cfg.tx_skipped_aliases]
    rx_active = [a for a in cfg.rx_by_alias.keys() if a not in cfg.rx_skipped_aliases]
    tx_on = sum(1 for a in tx_active if state.tx_online.get(a, False))
    rx_on = sum(1 for a in rx_active if state.rx_online.get(a, False))
    mode = "DRY_RUN" if cfg.amx_dry_run else "LIVE"
    return (
        f"DTSTATUS MODE={mode} RTI_CLIENTS={health.rti_clients} "
        f"TX_ONLINE={tx_on}/{len(tx_active)} RX_ONLINE={rx_on}/{len(rx_active)}"
    )


def build_device_status_lines(
    *,
    cfg: Config,
    state: ControllerState,
    health: HealthState,
) -> List[str]:
    """Includes DTSTATUS summary (logging/diagnostics). Prefer build_twoway_status_lines for RTI."""
    lines: List[str] = [format_dt_status_summary(cfg=cfg, state=state, health=health)]
    lines.extend(build_twoway_status_lines(cfg=cfg, state=state))
    return lines


def build_twoway_status_lines(*, cfg: Config, state: ControllerState) -> List[str]:
    """
    Lines pushed to RTI Two Way Strings — one logical message per line (LF framed).

    Omits DTSTATUS so RX string slots are not polluted; each DTTX/DTRXSUMMARY line
    is parsed independently when enableStopByte + stopChar %0a are set in the driver.
    """
    lines: List[str] = []
    for tx_alias in sorted(cfg.tx_by_alias.keys(), key=tx_alias_sort_key):
        if tx_alias in cfg.tx_skipped_aliases:
            continue
        lines.append(format_dt_tx_line(state=state, tx_alias=tx_alias))
    lines.append(format_dt_rx_summary_line(cfg=cfg, state=state))
    return lines


class RtiStatusReporter:
    """
    Push status to RTI Two Way Strings (TCP persistent link).

    - Per-TX DTTX lines (boolean-friendly key=value fields for Integration Designer)
    - One DTRXSUMMARY line for all RX (human-readable string; not per-RX booleans)
    """

    def __init__(
        self,
        *,
        cfg: Config,
        state: ControllerState,
        health: HealthState,
        runtime: RuntimeSettings,
        sender: RtiTwoWayTransport,
        interval_seconds: int,
        on_change: bool = True,
    ) -> None:
        self._cfg = cfg
        self._state = state
        self._health = health
        self._runtime = runtime
        self._sender = sender
        self._interval = max(1, int(interval_seconds))
        self._on_change = bool(on_change)
        self._last_sent: Dict[str, str] = {}
        self._loop_task: Optional[asyncio.Task[None]] = None
        self._push_task: Optional[asyncio.Task[None]] = None

    async def start(self) -> None:
        await self._sender.start()
        self._loop_task = asyncio.create_task(self._loop(), name="dt-rti-status-reporter")

    def schedule_push(self, *, force: bool = False) -> None:
        if not self._sender.enabled:
            return
        if self._push_task is not None and not self._push_task.done():
            if not force:
                return
        self._push_task = asyncio.create_task(
            self._push_changes(force=force),
            name="dt-rti-status-push",
        )

    async def _loop(self) -> None:
        while True:
            await asyncio.sleep(self._interval)
            if not self._runtime.rti_status_enabled:
                continue
            try:
                await self._push_all()
            except Exception:
                LOG.exception("RTI status periodic push failed")

    async def _push_all(self) -> None:
        lines = build_twoway_status_lines(cfg=self._cfg, state=self._state)
        self._last_sent = {line: line for line in lines}
        await self._sender.send_lines(lines)
        if LOG.isEnabledFor(logging.DEBUG):
            LOG.debug("RTI status: sent %d line(s) (full refresh)", len(lines))

    async def _push_changes(self, *, force: bool) -> None:
        if not self._runtime.rti_status_enabled:
            return
        if force or not self._on_change:
            await self._push_all()
            return
        lines = build_twoway_status_lines(cfg=self._cfg, state=self._state)
        changed = [line for line in lines if self._last_sent.get(line) != line]
        if not changed:
            return
        for line in changed:
            self._last_sent[line] = line
            await self._sender.send(line)
        if LOG.isEnabledFor(logging.DEBUG):
            LOG.debug("RTI status: sent %d changed line(s)", len(changed))
