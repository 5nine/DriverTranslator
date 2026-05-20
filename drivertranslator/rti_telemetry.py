from __future__ import annotations

import asyncio
import contextlib
import logging
import socket
from typing import TYPE_CHECKING, Awaitable, Callable, Optional

from .networking import open_connection

if TYPE_CHECKING:
    from .models import RuntimeSettings

LOG = logging.getLogger("drivertranslator")

OnInboundLine = Callable[[str], Awaitable[None]]


class RtiTwoWayTransport:
    """
    RTI Two Way Strings transport (TCP default, UDP optional).

    TCP (recommended): one persistent connection to the RTI processor. DriverTranslator
    writes CRLF-terminated status lines; a background reader accepts inbound lines from
    RTI (for future command strings on the same socket). Matches RTI TCP mode where the
    Connection boolean reflects link state.

    UDP: connectionless one-way status datagrams (RTI doc: effectively one-way).
    """

    def __init__(
        self,
        *,
        enabled: bool,
        protocol: str,
        host: Optional[str],
        port: int,
        bind_address: Optional[str] = None,
        on_inbound_line: Optional[OnInboundLine] = None,
        runtime: Optional["RuntimeSettings"] = None,
        connect_timeout_s: float = 5.0,
        reconnect_delay_s: float = 2.0,
    ) -> None:
        self._enabled = enabled and bool(host) and int(port) > 0
        self._runtime = runtime
        self._protocol = (protocol or "tcp").strip().lower()
        self._host = host or ""
        self._port = int(port)
        self._bind_address = bind_address
        self._on_inbound_line = on_inbound_line
        self._connect_timeout_s = max(0.5, float(connect_timeout_s))
        self._reconnect_delay_s = max(0.5, float(reconnect_delay_s))
        self._udp_transport: Optional[asyncio.DatagramTransport] = None
        self._udp_ready = asyncio.Event()
        self._tcp_reader: Optional[asyncio.StreamReader] = None
        self._tcp_writer: Optional[asyncio.StreamWriter] = None
        self._tcp_connected = asyncio.Event()
        self._tcp_send_lock = asyncio.Lock()
        self._tcp_task: Optional[asyncio.Task[None]] = None

    @property
    def enabled(self) -> bool:
        return self._enabled

    @property
    def protocol(self) -> str:
        return self._protocol

    async def start(self) -> None:
        if not self._enabled:
            return
        if self._protocol == "udp":
            await self._start_udp()
            return
        self._tcp_task = asyncio.create_task(self._tcp_loop(), name="dt-rti-twoway-tcp")

    async def _start_udp(self) -> None:
        loop = asyncio.get_running_loop()
        local = (self._bind_address, 0) if self._bind_address else None
        try:
            transport, _ = await loop.create_datagram_endpoint(
                lambda: asyncio.DatagramProtocol(),
                local_addr=local,
                family=socket.AF_INET,
            )
        except Exception as e:
            if local is None:
                raise
            LOG.warning(
                "RTI UDP bind failed for %r (%s); retrying wildcard IPv4 bind.",
                self._bind_address,
                e,
            )
            transport, _ = await loop.create_datagram_endpoint(
                lambda: asyncio.DatagramProtocol(),
                local_addr=("0.0.0.0", 0),
                family=socket.AF_INET,
            )
        self._udp_transport = transport  # type: ignore[assignment]
        self._udp_ready.set()

    def _telemetry_active(self) -> bool:
        if not self._enabled:
            return False
        if self._runtime is not None and not self._runtime.rti_status_enabled:
            return False
        return True

    async def _tcp_loop(self) -> None:
        while self._enabled:
            if not self._telemetry_active():
                self._tcp_connected.clear()
                await asyncio.sleep(1.0)
                continue
            reader: Optional[asyncio.StreamReader] = None
            writer: Optional[asyncio.StreamWriter] = None
            try:
                reader, writer = await open_connection(
                    self._host,
                    self._port,
                    timeout=self._connect_timeout_s,
                    local_addr=(self._bind_address, 0) if self._bind_address else None,
                )
                self._tcp_reader = reader
                self._tcp_writer = writer
                self._tcp_connected.set()
                LOG.info(
                    "RTI Two Way Strings TCP connected to %s:%d",
                    self._host,
                    self._port,
                )
                await self._tcp_read_loop(reader)
            except asyncio.CancelledError:
                raise
            except Exception:
                LOG.debug(
                    "RTI Two Way Strings TCP connect/read ended (%s:%d)",
                    self._host,
                    self._port,
                    exc_info=True,
                )
            finally:
                self._tcp_connected.clear()
                self._tcp_reader = None
                self._tcp_writer = None
                if writer is not None:
                    writer.close()
                    with contextlib.suppress(Exception):
                        await writer.wait_closed()
            await asyncio.sleep(self._reconnect_delay_s)

    async def _tcp_read_loop(self, reader: asyncio.StreamReader) -> None:
        while True:
            line = await _read_text_line(reader)
            if line is None:
                break
            if not line:
                continue
            LOG.debug("RTI Two Way -> %s", line)
            if self._on_inbound_line is not None:
                with contextlib.suppress(Exception):
                    await self._on_inbound_line(line)

    async def send(self, message: str) -> None:
        if not self._telemetry_active():
            return
        payload = (message.rstrip("\r\n") + "\r\n").encode("utf-8", errors="replace")
        if self._protocol == "udp":
            await self._udp_ready.wait()
            if self._udp_transport is not None:
                self._udp_transport.sendto(payload, (self._host, self._port))
            return
        try:
            await asyncio.wait_for(self._tcp_connected.wait(), timeout=self._connect_timeout_s)
        except asyncio.TimeoutError:
            LOG.debug(
                "RTI Two Way TCP not connected; dropped status line (%s:%d)",
                self._host,
                self._port,
            )
            return
        async with self._tcp_send_lock:
            writer = self._tcp_writer
            if writer is None:
                return
            try:
                writer.write(payload)
                await writer.drain()
            except Exception:
                LOG.debug("RTI Two Way TCP send failed", exc_info=True)


async def _read_text_line(reader: asyncio.StreamReader) -> Optional[str]:
    """Read one line terminated by LF, CRLF, or CR (RTI / telnet-style)."""
    try:
        data = await reader.readuntil(b"\n")
    except asyncio.IncompleteReadError:
        return None
    except asyncio.LimitOverrunError:
        return None
    text = data.decode("utf-8", errors="replace").strip("\r\n\u00a0 \t")
    if text.endswith("\r"):
        text = text.rstrip("\r")
    return text


async def handle_inbound_twoway_line(line: str) -> None:
    """
    Placeholder for RTI -> DriverTranslator command strings on the Two Way TCP link.
    Logs now; extend here when adding command dispatch (matrix, reboot, etc.).
    """
    LOG.info("RTI Two Way command (not handled yet): %s", line)
