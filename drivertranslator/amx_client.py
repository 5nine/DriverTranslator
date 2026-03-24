from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import Any, Dict, List, Optional, Tuple

from .amx_protocol import hdmi_enabled_from_status_fields, log_amx_inbound, parse_amx_status
from .networking import open_connection
from .utils import retry_delay_seconds

LOG = logging.getLogger("drivertranslator")


class AmxClient:
    def __init__(
        self,
        *,
        decoder_port: int,
        connect_timeout_ms: int,
        command_timeout_ms: int,
        bind_address: Optional[str] = None,
        set_retry_attempts: int = 1,
        set_retry_backoff_initial_ms: int = 0,
        set_retry_backoff_max_ms: int = 0,
        expanded_log: bool = False,
    ):
        self._decoder_port = decoder_port
        self._connect_timeout = connect_timeout_ms / 1000
        self._command_timeout = command_timeout_ms / 1000
        self._local_addr: Optional[Tuple[str, int]] = (bind_address, 0) if bind_address else None
        self._locks: Dict[str, asyncio.Lock] = {}
        self._set_retry_attempts = max(1, int(set_retry_attempts))
        self._set_retry_backoff_initial_ms = max(0, int(set_retry_backoff_initial_ms))
        self._set_retry_backoff_max_ms = max(0, int(set_retry_backoff_max_ms))
        self._expanded_log = bool(expanded_log)

    def set_expanded_log(self, enabled: bool) -> None:
        self._expanded_log = bool(enabled)

    def _lock_for(self, decoder_ip: str) -> asyncio.Lock:
        lock = self._locks.get(decoder_ip)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[decoder_ip] = lock
        return lock

    async def set_stream(self, *, decoder_ip: str, stream: int) -> None:
        if stream <= 0:
            raise ValueError(f"Invalid AMX stream id: {stream}")

        # AMX doc: port 50002 allows single connection at a time.
        async with self._lock_for(decoder_ip):
            await self._set_stream_locked_with_retry(decoder_ip=decoder_ip, stream=stream)

    async def set_hdmi_output(self, *, decoder_ip: str, enabled: bool) -> None:
        async with self._lock_for(decoder_ip):
            await self._set_hdmi_output_locked_with_retry(decoder_ip=decoder_ip, enabled=enabled)

    async def set_stream_and_get_hdmi_output(
        self, *, decoder_ip: str, stream: int, timeout_ms: int
    ) -> Optional[bool]:
        if stream <= 0:
            raise ValueError(f"Invalid AMX stream id: {stream}")
        async with self._lock_for(decoder_ip):
            return await self._set_stream_and_get_hdmi_output_locked_with_retry(
                decoder_ip=decoder_ip,
                stream=stream,
                timeout_ms=timeout_ms,
            )

    async def set_hdmi_output_and_get_hdmi_output(
        self, *, decoder_ip: str, enabled: bool, timeout_ms: int
    ) -> Optional[bool]:
        async with self._lock_for(decoder_ip):
            return await self._set_hdmi_output_and_get_hdmi_output_locked_with_retry(
                decoder_ip=decoder_ip,
                enabled=enabled,
                timeout_ms=timeout_ms,
            )

    async def send_command(self, *, decoder_ip: str, command: str) -> None:
        payload = command if command.endswith("\r") else (command + "\r")
        async with self._lock_for(decoder_ip):
            await self._send_locked(decoder_ip=decoder_ip, cmd=payload.encode("ascii"))

    async def send_command_and_get_hdmi_output(
        self, *, decoder_ip: str, command: str, timeout_ms: int
    ) -> Optional[bool]:
        payload = command if command.endswith("\r") else (command + "\r")
        async with self._lock_for(decoder_ip):
            return await self._send_and_query_hdmi_locked(
                decoder_ip=decoder_ip,
                cmd=payload.encode("ascii"),
                timeout_ms=timeout_ms,
            )

    async def send_command_with_status(
        self, *, decoder_ip: str, command: str, timeout_ms: int
    ) -> Dict[str, str]:
        payload = command if command.endswith("\r") else (command + "\r")
        async with self._lock_for(decoder_ip):
            return await self._send_and_read_status_locked(
                decoder_ip=decoder_ip,
                cmd=payload.encode("ascii"),
                timeout_ms=timeout_ms,
            )

    async def verify_stream(self, *, decoder_ip: str, expected_stream: int, timeout_ms: int) -> bool:
        # Stateless client: open a connection and query status
        try:
            reader, writer = await open_connection(
                decoder_ip,
                self._decoder_port,
                timeout=self._connect_timeout,
                local_addr=self._local_addr,
            )
        except Exception:
            return False

        try:
            writer.write(b"?\r")
            await writer.drain()
            data = b""
            try:
                data = await asyncio.wait_for(reader.read(4096), timeout=timeout_ms / 1000)
            except Exception:
                pass
            log_amx_inbound(
                enabled=self._expanded_log, decoder_ip=decoder_ip, decoder_port=self._decoder_port, data=data
            )
            parsed = parse_amx_status(data)
            got = parsed.get("STREAM")
            return got == str(expected_stream)
        finally:
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()

    async def get_hdmi_output(self, *, decoder_ip: str, timeout_ms: int) -> Optional[bool]:
        # Keep port 50002 command-safe: share the same per-RX lock as set_stream/set_hdmi_output.
        async with self._lock_for(decoder_ip):
            try:
                reader, writer = await open_connection(
                    decoder_ip,
                    self._decoder_port,
                    timeout=self._connect_timeout,
                    local_addr=self._local_addr,
                )
            except Exception:
                return None

            try:
                writer.write(b"?\r")
                await writer.drain()
                try:
                    data = await asyncio.wait_for(reader.read(4096), timeout=timeout_ms / 1000)
                except Exception:
                    return None
                log_amx_inbound(
                    enabled=self._expanded_log, decoder_ip=decoder_ip, decoder_port=self._decoder_port, data=data
                )
                parsed = parse_amx_status(data)
                hdmi_off = (parsed.get("HDMIOFF") or "").strip().lower()
                if hdmi_off == "on":
                    return False
                if hdmi_off == "off":
                    return True
                return None
            finally:
                writer.close()
                with contextlib.suppress(Exception):
                    await writer.wait_closed()

    async def _set_stream_locked(self, *, decoder_ip: str, stream: int) -> None:
        cmd = f"set:{stream}\r".encode("ascii")
        await self._send_locked(decoder_ip=decoder_ip, cmd=cmd)

    async def _set_hdmi_output_locked(self, *, decoder_ip: str, enabled: bool) -> None:
        cmd = b"hdmiOn\r" if enabled else b"hdmiOff\r"
        await self._send_locked(decoder_ip=decoder_ip, cmd=cmd)

    async def _send_and_query_hdmi_locked(
        self, *, decoder_ip: str, cmd: bytes, timeout_ms: int
    ) -> Optional[bool]:
        parsed = await self._send_and_read_status_locked(decoder_ip=decoder_ip, cmd=cmd, timeout_ms=timeout_ms)
        return hdmi_enabled_from_status_fields(parsed)

    async def _send_and_read_status_locked(
        self, *, decoder_ip: str, cmd: bytes, timeout_ms: int
    ) -> Dict[str, str]:
        LOG.info("AMX -> %s:%d %r", decoder_ip, self._decoder_port, cmd)
        try:
            reader, writer = await open_connection(
                decoder_ip,
                self._decoder_port,
                timeout=self._connect_timeout,
                local_addr=self._local_addr,
            )
        except Exception as e:
            raise ConnectionError(f"Failed to connect to AMX decoder {decoder_ip}:{self._decoder_port}: {e}") from e
        try:
            writer.write(cmd)
            await writer.drain()
            try:
                data = await asyncio.wait_for(reader.read(4096), timeout=timeout_ms / 1000)
            except Exception:
                return {}
            log_amx_inbound(
                enabled=self._expanded_log, decoder_ip=decoder_ip, decoder_port=self._decoder_port, data=data
            )
            return parse_amx_status(data)
        finally:
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()

    async def _send_locked(self, *, decoder_ip: str, cmd: bytes) -> None:
        LOG.info("AMX -> %s:%d %r", decoder_ip, self._decoder_port, cmd)
        try:
            reader, writer = await open_connection(
                decoder_ip,
                self._decoder_port,
                timeout=self._connect_timeout,
                local_addr=self._local_addr,
            )
        except Exception as e:
            raise ConnectionError(f"Failed to connect to AMX decoder {decoder_ip}:{self._decoder_port}: {e}") from e

        try:
            writer.write(cmd)
            await writer.drain()

            # Many AMX commands respond with a full status packet; we don't need it for the RTI ack,
            # but reading a little helps avoid leaving unread data.
            try:
                data = await asyncio.wait_for(reader.read(256), timeout=self._command_timeout)
                log_amx_inbound(
                    enabled=self._expanded_log, decoder_ip=decoder_ip, decoder_port=self._decoder_port, data=data
                )
            except Exception:
                pass
        finally:
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()

    async def _set_stream_locked_with_retry(self, *, decoder_ip: str, stream: int) -> None:
        last_exc: Optional[BaseException] = None
        for attempt in range(1, self._set_retry_attempts + 1):
            try:
                await self._set_stream_locked(decoder_ip=decoder_ip, stream=stream)
                return
            except Exception as e:
                last_exc = e
                if attempt >= self._set_retry_attempts:
                    raise
                delay = retry_delay_seconds(
                    attempt_index=attempt,
                    initial_ms=self._set_retry_backoff_initial_ms,
                    max_ms=self._set_retry_backoff_max_ms,
                )
                if delay > 0:
                    LOG.warning(
                        "AMX set_stream retry %d/%d for %s (after error: %s). Sleeping %.3fs",
                        attempt,
                        self._set_retry_attempts,
                        decoder_ip,
                        e,
                        delay,
                    )
                    await asyncio.sleep(delay)
        if last_exc is not None:
            raise last_exc

    async def _set_hdmi_output_locked_with_retry(self, *, decoder_ip: str, enabled: bool) -> None:
        last_exc: Optional[BaseException] = None
        for attempt in range(1, self._set_retry_attempts + 1):
            try:
                await self._set_hdmi_output_locked(decoder_ip=decoder_ip, enabled=enabled)
                return
            except Exception as e:
                last_exc = e
                if attempt >= self._set_retry_attempts:
                    raise
                delay = retry_delay_seconds(
                    attempt_index=attempt,
                    initial_ms=self._set_retry_backoff_initial_ms,
                    max_ms=self._set_retry_backoff_max_ms,
                )
                if delay > 0:
                    LOG.warning(
                        "AMX set_hdmi_output retry %d/%d for %s (after error: %s). Sleeping %.3fs",
                        attempt,
                        self._set_retry_attempts,
                        decoder_ip,
                        e,
                        delay,
                    )
                    await asyncio.sleep(delay)
        if last_exc is not None:
            raise last_exc

    async def _set_stream_and_get_hdmi_output_locked_with_retry(
        self, *, decoder_ip: str, stream: int, timeout_ms: int
    ) -> Optional[bool]:
        last_exc: Optional[BaseException] = None
        cmd = f"set:{stream}\r".encode("ascii")
        for attempt in range(1, self._set_retry_attempts + 1):
            try:
                return await self._send_and_query_hdmi_locked(decoder_ip=decoder_ip, cmd=cmd, timeout_ms=timeout_ms)
            except Exception as e:
                last_exc = e
                if attempt >= self._set_retry_attempts:
                    raise
                delay = retry_delay_seconds(
                    attempt_index=attempt,
                    initial_ms=self._set_retry_backoff_initial_ms,
                    max_ms=self._set_retry_backoff_max_ms,
                )
                if delay > 0:
                    await asyncio.sleep(delay)
        if last_exc is not None:
            raise last_exc
        return None

    async def _set_hdmi_output_and_get_hdmi_output_locked_with_retry(
        self, *, decoder_ip: str, enabled: bool, timeout_ms: int
    ) -> Optional[bool]:
        last_exc: Optional[BaseException] = None
        cmd = b"hdmiOn\r" if enabled else b"hdmiOff\r"
        for attempt in range(1, self._set_retry_attempts + 1):
            try:
                return await self._send_and_query_hdmi_locked(decoder_ip=decoder_ip, cmd=cmd, timeout_ms=timeout_ms)
            except Exception as e:
                last_exc = e
                if attempt >= self._set_retry_attempts:
                    raise
                delay = retry_delay_seconds(
                    attempt_index=attempt,
                    initial_ms=self._set_retry_backoff_initial_ms,
                    max_ms=self._set_retry_backoff_max_ms,
                )
                if delay > 0:
                    await asyncio.sleep(delay)
        if last_exc is not None:
            raise last_exc
        return None


# ---------------------------------------------------------------------------
# AMX client implementations (live, dry-run, persistent)
# ---------------------------------------------------------------------------
class DryRunAmxClient:
    def __init__(self, *, decoder_port: int, offline_decoders: Optional[List[str]] = None):
        self._decoder_port = decoder_port
        self._offline = {x.strip() for x in (offline_decoders or []) if str(x).strip()}
        self._sim_stream: Dict[str, int] = {}
        self._sim_hdmi_enabled: Dict[str, bool] = {}
        if self._offline:
            LOG.warning("AMX (dry-run) offline simulation enabled for %d decoder(s): %s", len(self._offline), sorted(self._offline))

    def _ensure_sim_state(self, decoder_ip: str) -> None:
        # Simulate AMX RX defaults at startup.
        self._sim_stream.setdefault(decoder_ip, 1)
        self._sim_hdmi_enabled.setdefault(decoder_ip, True)

    async def set_stream(self, *, decoder_ip: str, stream: int) -> None:
        if stream <= 0:
            raise ValueError(f"Invalid AMX stream id: {stream}")
        if decoder_ip in self._offline:
            LOG.error("AMX (dry-run) simulated OFFLINE decoder: %s:%d", decoder_ip, self._decoder_port)
            raise ConnectionError(f"(dry-run) Simulated offline decoder: {decoder_ip}:{self._decoder_port}")
        self._ensure_sim_state(decoder_ip)
        self._sim_stream[decoder_ip] = int(stream)
        cmd = f"set:{stream}\\r".encode("ascii")
        LOG.info("AMX (dry-run) -> %s:%d %r", decoder_ip, self._decoder_port, cmd)

    async def verify_stream(self, *, decoder_ip: str, expected_stream: int, timeout_ms: int) -> bool:
        if decoder_ip in self._offline:
            return False
        return True

    async def get_hdmi_output(self, *, decoder_ip: str, timeout_ms: int) -> Optional[bool]:
        _ = timeout_ms
        if decoder_ip in self._offline:
            return None
        self._ensure_sim_state(decoder_ip)
        return bool(self._sim_hdmi_enabled.get(decoder_ip, True))

    async def get_status_fields(self, *, decoder_ip: str, timeout_ms: int) -> Dict[str, str]:
        _ = timeout_ms
        if decoder_ip in self._offline:
            return {}
        self._ensure_sim_state(decoder_ip)
        return {
            "STREAM": str(self._sim_stream.get(decoder_ip, 1)),
            "HDMIOFF": "off" if self._sim_hdmi_enabled.get(decoder_ip, True) else "on",
        }

    async def set_hdmi_output(self, *, decoder_ip: str, enabled: bool) -> None:
        if decoder_ip in self._offline:
            LOG.error("AMX (dry-run) simulated OFFLINE decoder: %s:%d", decoder_ip, self._decoder_port)
            raise ConnectionError(f"(dry-run) Simulated offline decoder: {decoder_ip}:{self._decoder_port}")
        self._ensure_sim_state(decoder_ip)
        self._sim_hdmi_enabled[decoder_ip] = bool(enabled)
        cmd = b"hdmiOn\r" if enabled else b"hdmiOff\r"
        LOG.info("AMX (dry-run) -> %s:%d %r", decoder_ip, self._decoder_port, cmd)

    async def send_command(self, *, decoder_ip: str, command: str) -> None:
        if decoder_ip in self._offline:
            LOG.error("AMX (dry-run) simulated OFFLINE decoder: %s:%d", decoder_ip, self._decoder_port)
            raise ConnectionError(f"(dry-run) Simulated offline decoder: {decoder_ip}:{self._decoder_port}")
        self._ensure_sim_state(decoder_ip)
        c = command.strip().lower()
        if c.startswith("set:"):
            try:
                self._sim_stream[decoder_ip] = int(c.split(":", 1)[1].strip())
            except Exception:
                pass
        elif c == "hdmioff":
            self._sim_hdmi_enabled[decoder_ip] = False
        elif c == "hdmion":
            self._sim_hdmi_enabled[decoder_ip] = True
        payload = (command if command.endswith("\r") else (command + "\r")).encode("ascii")
        LOG.info("AMX (dry-run) -> %s:%d %r", decoder_ip, self._decoder_port, payload)

    async def send_command_and_get_hdmi_output(
        self, *, decoder_ip: str, command: str, timeout_ms: int
    ) -> Optional[bool]:
        _ = timeout_ms
        await self.send_command(decoder_ip=decoder_ip, command=command)
        return await self.get_hdmi_output(decoder_ip=decoder_ip, timeout_ms=timeout_ms)

    async def send_command_with_status(
        self, *, decoder_ip: str, command: str, timeout_ms: int
    ) -> Dict[str, str]:
        _ = timeout_ms
        await self.send_command(decoder_ip=decoder_ip, command=command)
        return await self.get_status_fields(decoder_ip=decoder_ip, timeout_ms=timeout_ms)


class PersistentAmxClient:
    """
    Maintains one TCP connection per decoder (port 50002).

    - Fast switching: no connect/disconnect per command.
    - Safe for 50002 single-connection limitation: we own the one socket.
    - Auto-reconnect and optional keepalive.
    """

    def __init__(
        self,
        *,
        decoder_port: int,
        connect_timeout_ms: int,
        command_timeout_ms: int,
        keepalive_seconds: int,
        bind_address: Optional[str] = None,
        set_queue_limit: int = 1,
        set_retry_attempts: int = 1,
        set_retry_backoff_initial_ms: int = 0,
        set_retry_backoff_max_ms: int = 0,
        expanded_log: bool = False,
    ):
        self._decoder_port = decoder_port
        self._connect_timeout = connect_timeout_ms / 1000
        self._command_timeout = command_timeout_ms / 1000
        self._keepalive_seconds = max(0, int(keepalive_seconds))
        self._local_addr: Optional[Tuple[str, int]] = (bind_address, 0) if bind_address else None
        self._set_queue_limit: int = max(1, int(set_queue_limit))
        self._set_retry_attempts: int = max(1, int(set_retry_attempts))
        self._set_retry_backoff_initial_ms: int = max(0, int(set_retry_backoff_initial_ms))
        self._set_retry_backoff_max_ms: int = max(0, int(set_retry_backoff_max_ms))
        self._expanded_log: bool = bool(expanded_log)

        self._workers: Dict[str, "DecoderWorker"] = {}
        self._workers_lock = asyncio.Lock()

    def set_expanded_log(self, enabled: bool) -> None:
        self._expanded_log = bool(enabled)
        for worker in self._workers.values():
            worker.set_expanded_log(enabled)

    async def set_stream(self, *, decoder_ip: str, stream: int) -> None:
        if stream <= 0:
            raise ValueError(f"Invalid AMX stream id: {stream}")

        worker = await self._get_worker(decoder_ip)
        await worker.send_set(stream)

    async def verify_stream(self, *, decoder_ip: str, expected_stream: int, timeout_ms: int) -> bool:
        worker = await self._get_worker(decoder_ip)
        return await worker.verify_stream(expected_stream=expected_stream, timeout_ms=timeout_ms)

    async def set_hdmi_output(self, *, decoder_ip: str, enabled: bool) -> None:
        worker = await self._get_worker(decoder_ip)
        await worker.send_hdmi_output(enabled=enabled)

    async def get_hdmi_output(self, *, decoder_ip: str, timeout_ms: int) -> Optional[bool]:
        worker = await self._get_worker(decoder_ip)
        return await worker.get_hdmi_output(timeout_ms=timeout_ms)

    async def send_command(self, *, decoder_ip: str, command: str) -> None:
        worker = await self._get_worker(decoder_ip)
        await worker.send_command(command=command)

    async def send_command_and_get_hdmi_output(
        self, *, decoder_ip: str, command: str, timeout_ms: int
    ) -> Optional[bool]:
        worker = await self._get_worker(decoder_ip)
        return await worker.send_command_and_get_hdmi_output(command=command, timeout_ms=timeout_ms)

    async def send_command_with_status(
        self, *, decoder_ip: str, command: str, timeout_ms: int
    ) -> Dict[str, str]:
        worker = await self._get_worker(decoder_ip)
        return await worker.send_command_with_status(command=command, timeout_ms=timeout_ms)

    async def _get_worker(self, decoder_ip: str) -> "DecoderWorker":
        async with self._workers_lock:
            w = self._workers.get(decoder_ip)
            if w is None:
                w = DecoderWorker(
                    decoder_ip=decoder_ip,
                    decoder_port=self._decoder_port,
                    connect_timeout=self._connect_timeout,
                    command_timeout=self._command_timeout,
                    keepalive_seconds=self._keepalive_seconds,
                    local_addr=self._local_addr,
                    set_retry_attempts=self._set_retry_attempts,
                    set_retry_backoff_initial_ms=self._set_retry_backoff_initial_ms,
                    set_retry_backoff_max_ms=self._set_retry_backoff_max_ms,
                    expanded_log=self._expanded_log,
                )
                w.set_queue_limit(getattr(self, "_set_queue_limit", 1))
                self._workers[decoder_ip] = w
                w.start()
            return w

    def connection_summary(self) -> Tuple[int, int]:
        """
        Returns (connected, total_known).
        total_known only counts decoders we've attempted to use in this process.
        """
        connected = 0
        total = 0
        for w in self._workers.values():
            total += 1
            if w.is_connected:
                connected += 1
        return connected, total


class DecoderWorker:
    def __init__(
        self,
        *,
        decoder_ip: str,
        decoder_port: int,
        connect_timeout: float,
        command_timeout: float,
        keepalive_seconds: int,
        local_addr: Optional[Tuple[str, int]] = None,
        set_retry_attempts: int = 1,
        set_retry_backoff_initial_ms: int = 0,
        set_retry_backoff_max_ms: int = 0,
        expanded_log: bool = False,
    ) -> None:
        self._decoder_ip = decoder_ip
        self._decoder_port = decoder_port
        self._connect_timeout = connect_timeout
        self._command_timeout = command_timeout
        self._keepalive_seconds = keepalive_seconds
        self._local_addr = local_addr
        self._set_retry_attempts = max(1, int(set_retry_attempts))
        self._set_retry_backoff_initial_ms = max(0, int(set_retry_backoff_initial_ms))
        self._set_retry_backoff_max_ms = max(0, int(set_retry_backoff_max_ms))
        self._expanded_log = bool(expanded_log)

        self._cond = asyncio.Condition()
        self._set_pending: Optional[Tuple[int, asyncio.Future[None]]] = None
        self._task: Optional[asyncio.Task[None]] = None

        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._connected_evt = asyncio.Event()
        self.is_connected: bool = False
        self._op_lock = asyncio.Lock()

    def set_expanded_log(self, enabled: bool) -> None:
        self._expanded_log = bool(enabled)

    def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._run(), name=f"amx-worker:{self._decoder_ip}")

    def set_queue_limit(self, limit: int) -> None:
        # latest-wins: limit is effectively 1; keep this hook for future extension
        _ = max(1, int(limit))

    async def send_set(self, stream: int) -> None:
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[None] = loop.create_future()
        async with self._cond:
            if self._set_pending is None:
                self._set_pending = (stream, fut)
            else:
                # Latest wins: supersede the previous pending set immediately.
                old_stream, old_fut = self._set_pending
                if not old_fut.done():
                    old_fut.set_exception(RuntimeError(f"Superseded by newer set:{stream} (dropped set:{old_stream})"))
                self._set_pending = (stream, fut)
            self._cond.notify_all()
        await fut

    async def verify_stream(self, *, expected_stream: int, timeout_ms: int) -> bool:
        # Query status on the existing socket.
        try:
            data = await self._query_status(timeout_ms=timeout_ms)
        except Exception:
            return False
        parsed = parse_amx_status(data)
        return parsed.get("STREAM") == str(expected_stream)

    async def get_hdmi_output(self, *, timeout_ms: int) -> Optional[bool]:
        try:
            data = await self._query_status(timeout_ms=timeout_ms)
        except Exception:
            return None
        parsed = parse_amx_status(data)
        hdmi_off = (parsed.get("HDMIOFF") or "").strip().lower()
        if hdmi_off == "on":
            return False
        if hdmi_off == "off":
            return True
        return None

    async def send_hdmi_output(self, *, enabled: bool) -> None:
        payload = b"hdmiOn\r" if enabled else b"hdmiOff\r"
        attempt = 1
        while True:
            try:
                await self._send_raw(payload, log_label="AMX")
                return
            except Exception as e:
                if attempt >= self._set_retry_attempts:
                    raise
                with contextlib.suppress(Exception):
                    await self._reconnect()
                delay = retry_delay_seconds(
                    attempt_index=attempt,
                    initial_ms=self._set_retry_backoff_initial_ms,
                    max_ms=self._set_retry_backoff_max_ms,
                )
                if delay > 0:
                    LOG.warning(
                        "AMX hdmi retry %d/%d for %s (after error: %s). Sleeping %.3fs",
                        attempt,
                        self._set_retry_attempts,
                        self._decoder_ip,
                        e,
                        delay,
                    )
                    await asyncio.sleep(delay)
                attempt += 1

    async def send_command(self, *, command: str) -> None:
        payload = (command if command.endswith("\r") else (command + "\r")).encode("ascii")
        await self._send_raw(payload, log_label="AMX")

    async def send_command_and_get_hdmi_output(
        self, *, command: str, timeout_ms: int
    ) -> Optional[bool]:
        parsed = await self.send_command_with_status(command=command, timeout_ms=timeout_ms)
        return hdmi_enabled_from_status_fields(parsed)

    async def send_command_with_status(self, *, command: str, timeout_ms: int) -> Dict[str, str]:
        payload = (command if command.endswith("\r") else (command + "\r")).encode("ascii")
        async with self._op_lock:
            await self._ensure_connected()
            assert self._writer is not None
            assert self._reader is not None
            LOG.info("AMX -> %s:%d %r", self._decoder_ip, self._decoder_port, payload)
            self._writer.write(payload)
            await self._writer.drain()
            try:
                data = await asyncio.wait_for(self._reader.read(4096), timeout=timeout_ms / 1000)
            except Exception:
                return {}
        log_amx_inbound(
            enabled=self._expanded_log, decoder_ip=self._decoder_ip, decoder_port=self._decoder_port, data=data
        )
        return parse_amx_status(data)

    async def _run(self) -> None:
        keepalive_task: Optional[asyncio.Task[None]] = None
        try:
            await self._ensure_connected()

            if self._keepalive_seconds > 0:
                keepalive_task = asyncio.create_task(self._keepalive_loop())

            while True:
                async with self._cond:
                    while self._set_pending is None:
                        await self._cond.wait()

                # Don't clear pending immediately: we want "latest wins" to be able
                # to supersede an in-flight send (including its retries).
                async with self._cond:
                    assert self._set_pending is not None
                    stream, fut = self._set_pending

                payload = f"set:{stream}\r".encode("ascii")
                attempt = 1
                while True:
                    # If superseded, abandon this send immediately.
                    async with self._cond:
                        if self._set_pending != (stream, fut):
                            break
                    try:
                        await self._send_raw(payload, log_label="AMX")
                        async with self._cond:
                            if self._set_pending == (stream, fut):
                                self._set_pending = None
                                if not fut.done():
                                    fut.set_result(None)
                        break
                    except Exception as e:
                        # If superseded, stop retrying.
                        async with self._cond:
                            if self._set_pending != (stream, fut):
                                break
                        if attempt >= self._set_retry_attempts:
                            async with self._cond:
                                if self._set_pending == (stream, fut):
                                    self._set_pending = None
                                    if not fut.done():
                                        fut.set_exception(e)
                            break

                        # Reconnect then retry after backoff.
                        with contextlib.suppress(Exception):
                            await self._reconnect()

                        delay = retry_delay_seconds(
                            attempt_index=attempt,
                            initial_ms=self._set_retry_backoff_initial_ms,
                            max_ms=self._set_retry_backoff_max_ms,
                        )
                        if delay > 0:
                            LOG.warning(
                                "AMX set_stream retry %d/%d for %s (after error: %s). Sleeping %.3fs",
                                attempt,
                                self._set_retry_attempts,
                                self._decoder_ip,
                                e,
                                delay,
                            )
                            await asyncio.sleep(delay)
                        attempt += 1
        finally:
            if keepalive_task is not None:
                keepalive_task.cancel()
                with contextlib.suppress(Exception):
                    await keepalive_task
            await self._close()

    async def _keepalive_loop(self) -> None:
        # AMX supports getStatus and "?" (doc shows "?\r" as getStatus alias).
        keepalive = b"?\r"
        while True:
            await asyncio.sleep(self._keepalive_seconds)
            try:
                await self._send_raw(keepalive, log_label="AMX keepalive")
            except Exception:
                # Connection might be down; worker main loop will reconnect on next command.
                await self._reconnect()

    async def _send_raw(self, payload: bytes, *, log_label: str) -> None:
        async with self._op_lock:
            await self._ensure_connected()
            assert self._writer is not None
            assert self._reader is not None

            LOG.info("%s -> %s:%d %r", log_label, self._decoder_ip, self._decoder_port, payload)
            self._writer.write(payload)
            await self._writer.drain()

            # Best-effort read to keep RX buffers clear; don't block routing on large status packets.
            with contextlib.suppress(Exception):
                data = await asyncio.wait_for(self._reader.read(256), timeout=self._command_timeout)
                log_amx_inbound(
                    enabled=self._expanded_log,
                    decoder_ip=self._decoder_ip,
                    decoder_port=self._decoder_port,
                    data=data,
                )

    async def _query_status(self, *, timeout_ms: int) -> bytes:
        async with self._op_lock:
            await self._ensure_connected()
            assert self._writer is not None
            assert self._reader is not None
            self._writer.write(b"?\r")
            await self._writer.drain()
            try:
                data = await asyncio.wait_for(self._reader.read(4096), timeout=timeout_ms / 1000)
                log_amx_inbound(
                    enabled=self._expanded_log,
                    decoder_ip=self._decoder_ip,
                    decoder_port=self._decoder_port,
                    data=data,
                )
                return data
            except Exception:
                return b""

    async def _ensure_connected(self) -> None:
        if self._writer is not None and not self._writer.is_closing():
            return
        await self._reconnect()

    async def _reconnect(self) -> None:
        await self._close()
        self._connected_evt.clear()
        self.is_connected = False

        try:
            reader, writer = await open_connection(
                self._decoder_ip,
                self._decoder_port,
                timeout=self._connect_timeout,
                local_addr=self._local_addr,
            )
        except Exception as e:
            raise ConnectionError(
                f"Failed to connect to AMX decoder {self._decoder_ip}:{self._decoder_port}: {e}"
            ) from e

        self._reader = reader
        self._writer = writer
        self._connected_evt.set()
        self.is_connected = True

    async def _close(self) -> None:
        if self._writer is not None:
            self._writer.close()
            with contextlib.suppress(Exception):
                await self._writer.wait_closed()
        self._reader = None
        self._writer = None
        self.is_connected = False
