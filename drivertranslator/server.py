from __future__ import annotations

import asyncio
import logging
import time
from pathlib import Path
from typing import Any, Optional

from .amx_client import AmxClient, DryRunAmxClient
from .amx_self_test import amx_self_test
from .http_status import handle_http_client
from .matrix_amx import RxStatusPoller, TxStatusPoller, refresh_rx_statuses, refresh_tx_statuses
from .models import Config, ControllerState, HealthState, ProblemState, RuntimeSettings
from .problem_reporter import LocalProblemReporter
from .rti_control_udp import RtiControlUdp
from .rti_tcp import handle_client
from .unknown_ctl import configure as unknown_ctl_configure, load_from_disk as unknown_ctl_load_from_disk
from .utils import tx_alias_sort_key

LOG = logging.getLogger("drivertranslator")


async def run_server(*, cfg: Config, config_path: str, listen: str, port: int) -> None:
    started_at = time.monotonic()
    _cfg_dir = Path(config_path).expanduser().resolve().parent
    unknown_ctl_configure(
        enabled=cfg.unknown_ctl_enabled,
        config_dir=_cfg_dir,
        persist_path=cfg.unknown_ctl_persist_path,
    )
    unknown_ctl_load_from_disk()
    # Optional RTI UDP control listener (e.g., reboot command)
    if cfg.rti_control_enabled and cfg.rti_control_port > 0:
        loop = asyncio.get_running_loop()
        bind = cfg.rti_control_bind_address or "0.0.0.0"
        await loop.create_datagram_endpoint(
            lambda: RtiControlUdp(cfg=cfg),
            local_addr=(bind, cfg.rti_control_port),
        )
        LOG.warning("RTI control UDP listening on %s:%d", bind, cfg.rti_control_port)
    if cfg.amx_bind_address:
        LOG.info("AMX outbound connections will bind to %s (AVoIP NIC)", cfg.amx_bind_address)

    if cfg.amx_dry_run:
        LOG.warning("AMX dry-run enabled: no TCP connections will be made.")
        if cfg.amx_dry_run_offline_decoders:
            LOG.warning(
                "AMX dry-run offline simulation decoders: %s",
                ", ".join(cfg.amx_dry_run_offline_decoders),
            )
        amx: Any = DryRunAmxClient(
            decoder_port=cfg.amx_decoder_port,
            offline_decoders=cfg.amx_dry_run_offline_decoders,
        )
    else:
        if cfg.amx_persistent:
            LOG.warning("Config has amx.persistent=true, but persistent mode is disabled; using non-persistent AMX client.")
        amx = AmxClient(
            decoder_port=cfg.amx_decoder_port,
            connect_timeout_ms=cfg.amx_connect_timeout_ms,
            command_timeout_ms=cfg.amx_command_timeout_ms,
            bind_address=cfg.amx_bind_address,
            set_retry_attempts=cfg.amx_set_retry_attempts,
            set_retry_backoff_initial_ms=cfg.amx_set_retry_backoff_initial_ms,
            set_retry_backoff_max_ms=cfg.amx_set_retry_backoff_max_ms,
            expanded_log=cfg.expanded_log,
        )

    state = ControllerState(cfg)
    if cfg.amx_dry_run:
        # Dry-run baseline: emulate RX status as STREAM:1 and HDMI enabled.
        tx_stream1_alias: Optional[str] = None
        for tx in cfg.tx_by_alias.values():
            if int(tx.amx_stream) == 1:
                tx_stream1_alias = tx.alias
                break
        if tx_stream1_alias is not None:
            for rx in cfg.rx_by_alias.values():
                if rx.alias in cfg.rx_skipped_aliases:
                    continue
                state.set_rx_all_media(rx_alias=rx.alias, tx_alias=tx_stream1_alias)
                state.set_rx_hdmi_output(rx.alias, True)
            LOG.info(
                "Dry-run startup seed: set all RX routes to %s (amx_stream=1), HDMI output ON",
                tx_stream1_alias,
            )
        else:
            LOG.warning("Dry-run startup seed skipped: no TX with amx_stream=1 in config")
    if cfg.amx_dry_run and cfg.amx_dry_run_offline_decoders:
        offline = {x.strip() for x in cfg.amx_dry_run_offline_decoders if str(x).strip()}
        for rx in cfg.rx_by_alias.values():
            if rx.alias in cfg.rx_skipped_aliases:
                continue
            if rx.amx_decoder_ip in offline:
                state.set_rx_online(rx.alias, False)
    elif not cfg.amx_dry_run:
        # In live mode, avoid optimistic "connected" until we have evidence.
        for rx in cfg.rx_by_alias.values():
            if rx.alias in cfg.rx_skipped_aliases:
                continue
            state.set_rx_online(rx.alias, False)
    health = HealthState()
    runtime = RuntimeSettings(cfg)
    problems = ProblemState()

    notifier = LocalProblemReporter(
        min_interval_seconds=10,
        repeat_suppression_seconds=300,
    )
    notifier.attach_problem_state(problems)

    async def _run_startup_self_test() -> None:
        # Run in background so web/RTI listeners come up immediately.
        try:
            res = await amx_self_test(cfg=cfg, amx=amx)
            unreachable = {str(x).strip() for x in (res.get("unreachable") or [])}
            # Reflect startup connectivity on the status page.
            for rx in cfg.rx_by_alias.values():
                if rx.alias in cfg.rx_skipped_aliases:
                    continue
                state.set_rx_online(rx.alias, rx.amx_decoder_ip not in unreachable)
            fail = res.get("unreachable") or []
            if fail:
                fail_l = list(fail)
                await notifier.problem(
                    "amx.selftest",
                    f"DT: ERROR AMX self-test: {res.get('ok', 0)}/{res.get('total', 0)} reachable. Unreachable: {', '.join(fail_l[:5])}"
                    + (" ..." if len(fail_l) > 5 else ""),
                )
        except Exception:
            LOG.exception("Startup AMX self-test failed")

    async def _prime_amx_status_once() -> None:
        # One-time startup status prime keeps the page accurate even when periodic pollers are disabled.
        if cfg.amx_dry_run:
            return
        try:
            await refresh_tx_statuses(cfg=cfg, state=state, runtime=runtime)
            offline_txs = sorted(
                [
                    tx_alias
                    for tx_alias in cfg.tx_by_alias.keys()
                    if tx_alias not in cfg.tx_skipped_aliases and not state.tx_online.get(tx_alias, False)
                ],
                key=tx_alias_sort_key,
            )
            if offline_txs:
                await notifier.problem(
                    "amx.txstatus.startup",
                    f"DT: ERROR AMX TX startup status poll: {len(offline_txs)}/{max(1, len(cfg.tx_by_alias) - len(cfg.tx_skipped_aliases))} offline. "
                    + ", ".join(offline_txs[:5])
                    + (" ..." if len(offline_txs) > 5 else ""),
                )
        except Exception:
            LOG.exception("Startup AMX TX status poll failed")
        try:
            await refresh_rx_statuses(cfg=cfg, state=state, runtime=runtime)
        except Exception:
            LOG.exception("Startup AMX RX status poll failed")

    if cfg.http_status_enabled:
        http_server = await asyncio.start_server(
            lambda r, w: handle_http_client(
                r,
                w,
                cfg=cfg,
                health=health,
                amx=amx,
                state=state,
                runtime=runtime,
                problems=problems,
                started_at=started_at,
                config_path=config_path,
                amx_self_test=amx_self_test,
                notifier=notifier,
            ),
            host=cfg.http_status_bind,
            port=cfg.http_status_port,
        )
        addrs = ", ".join(str(sock.getsockname()) for sock in (http_server.sockets or []))
        LOG.info("HTTP status listening on %s", addrs)

    server = await asyncio.start_server(
        lambda r, w: handle_client(cfg, amx, state, notifier, health, runtime, r, w),
        host=listen,
        port=port,
    )

    addrs = ", ".join(str(sock.getsockname()) for sock in (server.sockets or []))
    LOG.info("Listening on %s", addrs)

    tx_poller = TxStatusPoller(cfg=cfg, state=state, runtime=runtime)
    rx_poller = RxStatusPoller(cfg=cfg, state=state, runtime=runtime)
    # Start pollers after listeners are up so UI/RTI sockets become available first.
    asyncio.create_task(tx_poller.start())
    asyncio.create_task(rx_poller.start())

    # Optional AMX self-test on startup (problems-only notification), non-blocking.
    if cfg.amx_self_test_on_start:
        asyncio.create_task(_run_startup_self_test())
    # Always run one status prime in live mode; periodic TX/RX polling toggles still control ongoing loops.
    asyncio.create_task(_prime_amx_status_once())

    async with server:
        await server.serve_forever()
