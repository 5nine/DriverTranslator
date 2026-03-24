from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import Any, Dict, List, Optional, Tuple

from .amx_protocol import hdmi_enabled_from_status_fields, log_amx_inbound, parse_amx_status
from .http_status import TX_STATUS_POLL_INTERVAL_SECONDS
from .models import Config, ControllerState, RuntimeSettings, Rx
from .networking import open_connection
from .protocol_helpers import lookup_rx, lookup_tx
from .utils import rx_alias_sort_key, tx_alias_sort_key

LOG = logging.getLogger("drivertranslator")

async def handle_matrix_set(
    cfg: Config, amx: Any, state: ControllerState, cmd: str, timeout_ms: int
) -> Tuple[bool, str, List[Tuple[str, str, str]], Dict[str, Dict[str, str]]]:
    # cmd: "matrix set <TX> <RX1> <RX2> ... <RXn>"
    parts = cmd.split()
    if len(parts) < 4:
        return False, "unknown command", [], {}
    if parts[0].lower() != "matrix" or parts[1].lower() != "set":
        return False, "unknown command", [], {}

    tx_token = parts[2]
    rx_tokens = parts[3:]

    tx = lookup_tx(cfg, tx_token)
    if tx is None:
        # Allow explicit NULL routing in WyreStorm API
        if tx_token.upper() == "NULL":
            tx = None
        else:
            return False, "unknown command", [], {}

    rxs: List[Rx] = []
    for token in rx_tokens:
        rx = lookup_rx(cfg, token)
        if rx is None:
            return False, "unknown command", [], {}
        rxs.append(rx)

    failures: List[Tuple[str, str, str]] = []
    status_by_rx: Dict[str, Dict[str, str]] = {}
    if tx is not None:
        failures, status_by_rx = await apply_amx_command_to_rx_aliases(
            cfg=cfg,
            amx=amx,
            state=state,
            rx_aliases=[rx.alias for rx in rxs],
            command=f"set:{tx.amx_stream}",
            timeout_ms=timeout_ms,
        )
    else:
        LOG.info("AMX routing skipped: NULL assignment requested")

    # WyreStorm ack is a "command mirror"
    return True, cmd, failures, status_by_rx


async def refresh_hdmi_outputs(*, cfg: Config, amx: Any, state: ControllerState, timeout_ms: int) -> None:
    if not hasattr(amx, "get_hdmi_output"):
        return
    rx_aliases = sorted(
        [a for a in cfg.rx_by_alias.keys() if a not in cfg.rx_skipped_aliases],
        key=rx_alias_sort_key,
    )
    if not rx_aliases:
        return
    results = await asyncio.gather(
        *(
            amx.get_hdmi_output(
                decoder_ip=cfg.rx_by_alias[rx_alias].amx_decoder_ip,
                timeout_ms=timeout_ms,
            )
            for rx_alias in rx_aliases
        ),
        return_exceptions=True,
    )
    for rx_alias, res in zip(rx_aliases, results):
        if isinstance(res, BaseException):
            state.set_rx_online(rx_alias, False)
            state.set_rx_hdmi_output(rx_alias, None)
        else:
            state.set_rx_hdmi_output(rx_alias, res if isinstance(res, bool) else None)


async def refresh_hdmi_outputs_for_aliases(
    *,
    cfg: Config,
    amx: Any,
    state: ControllerState,
    timeout_ms: int,
    rx_aliases: List[str],
) -> None:
    if not hasattr(amx, "get_hdmi_output"):
        return
    wanted = [a for a in rx_aliases if a in cfg.rx_by_alias and a not in cfg.rx_skipped_aliases]
    if not wanted:
        return
    results = await asyncio.gather(
        *(
            amx.get_hdmi_output(
                decoder_ip=cfg.rx_by_alias[rx_alias].amx_decoder_ip,
                timeout_ms=timeout_ms,
            )
            for rx_alias in wanted
        ),
        return_exceptions=True,
    )
    for rx_alias, res in zip(wanted, results):
        if isinstance(res, BaseException):
            state.set_rx_online(rx_alias, False)
            state.set_rx_hdmi_output(rx_alias, None)
        else:
            state.set_rx_hdmi_output(rx_alias, res if isinstance(res, bool) else None)


async def read_amx_status_fields_from_ip(
    *,
    host: str,
    port: int,
    connect_timeout_ms: int,
    timeout_ms: int,
    local_addr: Optional[Tuple[str, int]],
    expanded_log: bool,
) -> Dict[str, str]:
    reader: Optional[asyncio.StreamReader] = None
    writer: Optional[asyncio.StreamWriter] = None
    try:
        reader, writer = await open_connection(
            host,
            port,
            timeout=max(0.2, connect_timeout_ms / 1000),
            local_addr=local_addr,
        )
        writer.write(b"?\r")
        await writer.drain()
        data = await asyncio.wait_for(reader.read(4096), timeout=max(0.2, timeout_ms / 1000))
        log_amx_inbound(enabled=expanded_log, decoder_ip=host, decoder_port=port, data=data)
        return parse_amx_status(data)
    finally:
        if writer is not None:
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()


async def refresh_tx_statuses(*, cfg: Config, state: ControllerState, runtime: RuntimeSettings) -> None:
    tx_aliases = sorted(
        [a for a in cfg.tx_by_alias.keys() if a not in cfg.tx_skipped_aliases],
        key=tx_alias_sort_key,
    )
    if not tx_aliases:
        return

    if cfg.amx_dry_run:
        for tx_alias in tx_aliases:
            tx = cfg.tx_by_alias[tx_alias]
            fields = {
                "STREAM": str(tx.amx_stream),
                "PLAYMODE": "live",
                "MUTE": "0",
                "HDMIINPUT": "connected",
                "INPUTRES": "1920x1080",
            }
            state.set_tx_online(tx_alias, True)
            state.set_tx_status_fields(tx_alias, fields)
        return

    local_addr = (cfg.amx_bind_address, 0) if cfg.amx_bind_address else None
    timeout_ms = max(200, min(5000, int(runtime.amx_verify_timeout_ms)))
    tasks = []
    for tx_alias in tx_aliases:
        tx = cfg.tx_by_alias[tx_alias]
        if not tx.ip:
            tasks.append(None)
            continue
        tasks.append(
            read_amx_status_fields_from_ip(
                host=tx.ip,
                port=cfg.amx_decoder_port,
                connect_timeout_ms=cfg.amx_connect_timeout_ms,
                timeout_ms=timeout_ms,
                local_addr=local_addr,
                expanded_log=runtime.expanded_log,
            )
        )

    awaited = [t for t in tasks if t is not None]
    results: List[Any] = []
    if awaited:
        results = list(await asyncio.gather(*awaited, return_exceptions=True))
    idx = 0
    for tx_alias, task in zip(tx_aliases, tasks):
        if task is None:
            state.set_tx_online(tx_alias, False)
            state.set_tx_status_fields(tx_alias, {})
            continue
        res = results[idx]
        idx += 1
        if isinstance(res, BaseException):
            state.set_tx_online(tx_alias, False)
            state.set_tx_status_fields(tx_alias, {})
        else:
            fields = res if isinstance(res, dict) else {}
            state.set_tx_online(tx_alias, bool(fields))
            state.set_tx_status_fields(tx_alias, fields)


class TxStatusPoller:
    def __init__(self, *, cfg: Config, state: ControllerState, runtime: RuntimeSettings) -> None:
        self._cfg = cfg
        self._state = state
        self._runtime = runtime
        self._task: Optional[asyncio.Task[None]] = None

    async def start(self) -> None:
        await refresh_tx_statuses(cfg=self._cfg, state=self._state, runtime=self._runtime)
        self._task = asyncio.create_task(self._loop(), name="dt-tx-status-poller")

    async def _loop(self) -> None:
        while True:
            await asyncio.sleep(TX_STATUS_POLL_INTERVAL_SECONDS)
            try:
                await refresh_tx_statuses(cfg=self._cfg, state=self._state, runtime=self._runtime)
            except Exception:
                LOG.exception("TX status poll failed")


async def apply_amx_command_to_rx_aliases(
    *,
    cfg: Config,
    amx: Any,
    state: ControllerState,
    rx_aliases: List[str],
    command: str,
    timeout_ms: int,
) -> Tuple[List[Tuple[str, str, str]], Dict[str, Dict[str, str]]]:
    rx_aliases_active = [a for a in rx_aliases if a in cfg.rx_by_alias and a not in cfg.rx_skipped_aliases]
    if not rx_aliases_active:
        return [], {}
    failures: List[Tuple[str, str, str]] = []
    status_by_rx: Dict[str, Dict[str, str]] = {}
    if hasattr(amx, "send_command_with_status"):
        results = await asyncio.gather(
            *(
                amx.send_command_with_status(
                    decoder_ip=cfg.rx_by_alias[a].amx_decoder_ip,
                    command=command,
                    timeout_ms=timeout_ms,
                )
                for a in rx_aliases_active
            ),
            return_exceptions=True,
        )
        for a, res in zip(rx_aliases_active, results):
            if isinstance(res, BaseException):
                failures.append((a, cfg.rx_by_alias[a].amx_decoder_ip, str(res)))
                state.set_rx_online(a, False)
                state.set_rx_hdmi_output(a, None)
            else:
                state.set_rx_online(a, True)
                fields = res if isinstance(res, dict) else {}
                status_by_rx[a] = fields
                state.set_rx_hdmi_output(a, hdmi_enabled_from_status_fields(fields))
        return failures, status_by_rx

    if hasattr(amx, "send_command"):
        results = await asyncio.gather(
            *(
                amx.send_command(
                    decoder_ip=cfg.rx_by_alias[a].amx_decoder_ip,
                    command=command,
                )
                for a in rx_aliases_active
            ),
            return_exceptions=True,
        )
        for a, res in zip(rx_aliases_active, results):
            if isinstance(res, BaseException):
                failures.append((a, cfg.rx_by_alias[a].amx_decoder_ip, str(res)))
                state.set_rx_online(a, False)
                state.set_rx_hdmi_output(a, None)
            else:
                state.set_rx_online(a, True)
    return failures, status_by_rx