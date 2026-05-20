from __future__ import annotations

import asyncio
import logging
from typing import Dict, List, Optional, Tuple

# (status, hdmi_state, tx_state) for RTI Two Way Strings TX lines
TxRtiFields = Tuple[str, str, str]

from .models import Config, ControllerState, HealthState, RuntimeSettings
from .protocol_helpers import classify_rx_hdmi_sink, classify_tx_input
from .rti_telemetry import RtiTwoWayTransport
from .utils import rx_alias_sort_key, tx_alias_sort_key

LOG = logging.getLogger("drivertranslator")


def _tri_token(v: Optional[bool], *, on: str, off: str, unknown: str = "UNKNOWN") -> str:
    if v is True:
        return on
    if v is False:
        return off
    return unknown


def build_rx_rti_fields(*, state: ControllerState, rx_alias: str) -> TxRtiFields:
    """
    RTI-facing RX telemetry (per receiver / decoder sink).

    - status: ok | error
    - hdmi-state: connected | disconnected | no signal | null (null when RX offline)
    - rx-state: connected | disconnected  (AMX TCP reachable)

    hdmi-state uses AMX HDMISTATUS (sink): TV/monitor detected vs off/detached.
    """
    rx_state = "connected" if state.rx_online.get(rx_alias, True) else "disconnected"
    if rx_state == "disconnected":
        return ("error", "null", "disconnected")

    fields = state.rx_status_fields.get(rx_alias, {})
    hdmi_state = classify_rx_hdmi_sink(fields)
    status = "ok" if hdmi_state == "connected" else "error"
    return (status, hdmi_state, rx_state)


def format_dt_rx_line(*, state: ControllerState, rx_alias: str) -> str:
    status, hdmi_state, rx_state = build_rx_rti_fields(state=state, rx_alias=rx_alias)
    return (
        f"DTRX {rx_alias} status={status} hdmi-state={hdmi_state} rx-state={rx_state}"
    )


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
    lines: List[str] = [format_dt_status_summary(cfg=cfg, state=state, health=health)]
    for tx_alias in sorted(cfg.tx_by_alias.keys(), key=tx_alias_sort_key):
        if tx_alias in cfg.tx_skipped_aliases:
            continue
        lines.append(format_dt_tx_line(state=state, tx_alias=tx_alias))
    for rx_alias in sorted(cfg.rx_by_alias.keys(), key=rx_alias_sort_key):
        if rx_alias in cfg.rx_skipped_aliases:
            continue
        lines.append(format_dt_rx_line(state=state, rx_alias=rx_alias))
    return lines


class RtiStatusReporter:
    """
    Push per-TX/RX status lines to RTI Two Way Strings (TCP persistent link).

    - Periodic full refresh every interval_seconds
    - On-change pushes after AMX polls / matrix updates (deduped per line)
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
        lines = build_device_status_lines(cfg=self._cfg, state=self._state, health=self._health)
        self._last_sent = {line: line for line in lines}
        for line in lines:
            await self._sender.send(line)
        if LOG.isEnabledFor(logging.DEBUG):
            LOG.debug("RTI status: sent %d line(s) (full refresh)", len(lines))

    async def _push_changes(self, *, force: bool) -> None:
        if not self._runtime.rti_status_enabled:
            return
        if force or not self._on_change:
            await self._push_all()
            return
        lines = build_device_status_lines(cfg=self._cfg, state=self._state, health=self._health)
        changed = [line for line in lines if self._last_sent.get(line) != line]
        if not changed:
            return
        for line in changed:
            self._last_sent[line] = line
            await self._sender.send(line)
        if LOG.isEnabledFor(logging.DEBUG):
            LOG.debug("RTI status: sent %d changed line(s)", len(changed))
