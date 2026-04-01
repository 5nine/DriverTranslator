from __future__ import annotations

import asyncio
import contextlib
import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from .amx_protocol import (
    hdmi_enabled_from_status_fields,
    hdmi_link_connected_from_status_fields,
    log_amx_inbound,
    parse_amx_status,
)
from .constants import RX_STATUS_POLL_INTERVAL_SECONDS, TX_STATUS_POLL_INTERVAL_SECONDS
from .models import Config, ControllerState, RuntimeSettings, Rx
from .networking import open_connection
from .problem_reporter import LocalProblemReporter
from .protocol_helpers import lookup_rx, lookup_tx, tx_alias_from_amx_stream
from .utils import rx_alias_sort_key, tx_alias_sort_key

LOG = logging.getLogger("drivertranslator")


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
                state.set_rx_hdmi_link(a, None)
            else:
                state.set_rx_online(a, True)
                fields = res if isinstance(res, dict) else {}
                status_by_rx[a] = fields
                state.set_rx_hdmi_output(a, hdmi_enabled_from_status_fields(fields))
                state.set_rx_hdmi_link(a, hdmi_link_connected_from_status_fields(fields))
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


@dataclass(frozen=True)
class MatrixSetLineResult:
    """Outcome of primary `matrix set <TX> <RX...>` (same path as RTI TCP)."""

    rti_ok: bool
    rti_response: str
    failures: Tuple[Tuple[str, str, str], ...]
    exception_occurred: bool


async def process_matrix_set_line(
    cfg: Config,
    amx: Any,
    state: ControllerState,
    line: str,
    timeout_ms: int,
    runtime: RuntimeSettings,
    notifier: LocalProblemReporter,
) -> MatrixSetLineResult:
    """
    Run AMX routing and ControllerState updates for a primary matrix set line.
    Matches RTI TCP behavior (including mirror-on-exception for drivers).
    """
    parts = line.split()
    parts_lower = [p.lower() for p in parts]
    ok = False
    resp = "unknown command"
    failures: List[Tuple[str, str, str]] = []
    status_by_rx: Dict[str, Dict[str, str]] = {}
    exception_occurred = False

    if len(parts) < 4 or parts_lower[:2] != ["matrix", "set"]:
        return MatrixSetLineResult(
            rti_ok=False,
            rti_response="unknown command",
            failures=(),
            exception_occurred=False,
        )

    tx_token = parts[2]
    tx_obj_for_log = lookup_tx(cfg, tx_token) if tx_token.upper() != "NULL" else None

    try:
        ok, resp, failures, status_by_rx = await handle_matrix_set(
            cfg, amx, state, line, timeout_ms
        )
        if ok:
            tx_alias: Optional[str]
            if tx_token.upper() == "NULL":
                tx_alias = None
            else:
                tx_o = lookup_tx(cfg, tx_token)
                tx_alias = tx_o.alias if tx_o is not None else None

            rx_aliases: List[str] = []
            for tok in parts[3:]:
                rx_obj = lookup_rx(cfg, tok)
                if rx_obj is not None:
                    rx_aliases.append(rx_obj.alias)
            state.set_all_media(tx_alias=tx_alias, rx_aliases=rx_aliases)
            if tx_alias is not None:
                failed_rx = {rx_a for (rx_a, _ip, _err) in failures}
                for rx_a in rx_aliases:
                    if rx_a in failed_rx:
                        state.set_rx_all_media(rx_alias=rx_a, tx_alias=None)
                        continue
                    stream_reported = (status_by_rx.get(rx_a, {}).get("STREAM") or "").strip()
                    if not stream_reported:
                        state.set_rx_all_media(rx_alias=rx_a, tx_alias=tx_alias)
                    else:
                        amx_tx_alias = tx_alias_from_amx_stream(cfg, stream_reported)
                        state.set_rx_all_media(
                            rx_alias=rx_a,
                            tx_alias=(amx_tx_alias if amx_tx_alias is not None else tx_alias),
                        )

            if (not cfg.amx_dry_run) and runtime.amx_verify_after_set and (not runtime.amx_rx_poll_enabled) and tx_alias is not None:
                tx_obj2 = lookup_tx(cfg, tx_alias)
                expected = str(tx_obj2.amx_stream) if tx_obj2 is not None else None
                if expected:
                    for rx_a in rx_aliases:
                        ip = cfg.rx_by_alias[rx_a].amx_decoder_ip
                        got = (status_by_rx.get(rx_a, {}).get("STREAM") or "").strip()
                        if got != expected:
                            await notifier.problem(
                                f"amx.verify.{ip}",
                                f"DT: ERROR AMX verify failed: {rx_a} expected STREAM {expected}",
                            )
    except Exception as e:
        LOG.exception("AMX routing failed")
        await notifier.problem("amx.route", f"DT: ERROR AMX route failed: {e}")
        exception_occurred = True
        ok, resp = True, line

    if failures:
        for (rx_a, ip, err) in failures:
            stream_s = str(tx_obj_for_log.amx_stream) if tx_obj_for_log is not None else "<unknown>"
            LOG.error(
                "AMX SEND FAIL route decoder=%s rx=%s cmd=%r err=%s",
                ip,
                rx_a,
                f"set:{stream_s}",
                err,
            )
            state.set_rx_online(rx_a, False)
            await notifier.problem(f"amx.set.{ip}", f"DT: ERROR AMX route failed: {rx_a} ({ip}): {err}")
        await notifier.problem(
            "amx.route.partial",
            "DT: ERROR AMX route failed on: "
            + ", ".join(f"{rx_a}({ip})" for (rx_a, ip, _e) in failures[:3])
            + (" ..." if len(failures) > 3 else ""),
        )
    else:
        try:
            for tok in parts[3:]:
                rx_obj = lookup_rx(cfg, tok)
                if rx_obj is not None:
                    state.set_rx_online(rx_obj.alias, True)
        except Exception:
            pass

    return MatrixSetLineResult(
        rti_ok=ok,
        rti_response=resp if ok else "unknown command",
        failures=tuple(failures),
        exception_occurred=exception_occurred,
    )


async def refresh_rx_statuses(*, cfg: Config, state: ControllerState, runtime: RuntimeSettings) -> None:
    rx_aliases = sorted(
        [a for a in cfg.rx_by_alias.keys() if a not in cfg.rx_skipped_aliases],
        key=rx_alias_sort_key,
    )
    if not rx_aliases:
        return
    local_addr = (cfg.amx_bind_address, 0) if cfg.amx_bind_address else None
    timeout_ms = max(200, min(5000, int(runtime.amx_verify_timeout_ms)))
    tasks = []
    for rx_alias in rx_aliases:
        rx = cfg.rx_by_alias[rx_alias]
        tasks.append(
            read_amx_status_fields_from_ip(
                host=rx.amx_decoder_ip,
                port=cfg.amx_decoder_port,
                connect_timeout_ms=cfg.amx_connect_timeout_ms,
                timeout_ms=timeout_ms,
                local_addr=local_addr,
                expanded_log=runtime.expanded_log,
                log_payload=False,
                log_errors=True,
            )
        )
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for rx_alias, res in zip(rx_aliases, results):
        if isinstance(res, BaseException):
            state.set_rx_online(rx_alias, False)
            state.set_rx_hdmi_output(rx_alias, None)
            state.set_rx_hdmi_link(rx_alias, None)
            continue
        fields = res if isinstance(res, dict) else {}
        state.set_rx_online(rx_alias, bool(fields))
        state.set_rx_hdmi_output(rx_alias, hdmi_enabled_from_status_fields(fields))
        state.set_rx_hdmi_link(rx_alias, hdmi_link_connected_from_status_fields(fields))
        stream_reported = (fields.get("STREAM") or "").strip()
        if stream_reported:
            amx_tx_alias = tx_alias_from_amx_stream(cfg, stream_reported)
            if amx_tx_alias is not None:
                state.set_breakaway(kind="video", tx_alias=amx_tx_alias, rx_aliases=[rx_alias])


class RxStatusPoller:
    def __init__(self, *, cfg: Config, state: ControllerState, runtime: RuntimeSettings) -> None:
        self._cfg = cfg
        self._state = state
        self._runtime = runtime
        self._task: Optional[asyncio.Task[None]] = None

    async def start(self) -> None:
        if self._runtime.amx_rx_poll_enabled:
            await refresh_rx_statuses(cfg=self._cfg, state=self._state, runtime=self._runtime)
        self._task = asyncio.create_task(self._loop(), name="dt-rx-status-poller")

    async def _loop(self) -> None:
        while True:
            await asyncio.sleep(RX_STATUS_POLL_INTERVAL_SECONDS)
            if not self._runtime.amx_rx_poll_enabled:
                continue
            try:
                await refresh_rx_statuses(cfg=self._cfg, state=self._state, runtime=self._runtime)
            except Exception:
                LOG.exception("RX status poll failed")


async def read_amx_status_fields_from_ip(
    *,
    host: str,
    port: int,
    connect_timeout_ms: int,
    timeout_ms: int,
    local_addr: Optional[Tuple[str, int]],
    expanded_log: bool,
    log_payload: bool = True,
    log_errors: bool = False,
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
        log_amx_inbound(
            enabled=expanded_log,
            decoder_ip=host,
            decoder_port=port,
            data=data,
            log_payload=log_payload,
        )
        return parse_amx_status(data)
    except Exception as e:
        if log_errors:
            LOG.warning("AMX status poll failed for %s:%d: %s", host, port, e)
        raise
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
    # TX polling timeout should follow AMX command/read timeout, not post-route verify tuning.
    timeout_ms = max(200, min(5000, int(cfg.amx_command_timeout_ms)))
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
                log_payload=False,
                log_errors=True,
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
