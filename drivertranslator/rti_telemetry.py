from __future__ import annotations

import asyncio
import contextlib
import logging
import socket
from typing import TYPE_CHECKING, Awaitable, Callable, Optional

if TYPE_CHECKING:
    from .models import RuntimeSettings

LOG = logging.getLogger("drivertranslator")

# RTI Two Way stopChar is %0a (LF). Use LF-only so each status line is one framed message.
RTI_TWOWAY_LINE_END = b"\n"

OnInboundLine = Callable[[str], Awaitable[None]]
OnClientEvent = Callable[[], Awaitable[None]]


class RtiTwoWayTransport:
    """
    RTI Two Way Strings transport (TCP default, UDP optional).

    TCP (recommended): DriverTranslator listens; RTI XP connects as TCP client
    (Two Way "TCP Connection" to integrion IP:port). One active client at a time;
    CRLF-terminated status lines are pushed on that socket; inbound lines from RTI
    are read for future command strings.

    UDP: connectionless one-way status datagrams to host:port (RTI doc: effectively one-way).
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
        on_client_connected: Optional[OnClientEvent] = None,
        on_client_disconnected: Optional[OnClientEvent] = None,
        runtime: Optional["RuntimeSettings"] = None,
        connect_timeout_s: float = 5.0,
        reconnect_delay_s: float = 2.0,
    ) -> None:
        self._runtime = runtime
        self._protocol = (protocol or "tcp").strip().lower()
        self._host = host or ""
        self._port = int(port)
        self._bind_address = bind_address
        self._on_inbound_line = on_inbound_line
        self._on_client_connected = on_client_connected
        self._on_client_disconnected = on_client_disconnected
        self._connect_timeout_s = max(0.5, float(connect_timeout_s))
        self._reconnect_delay_s = max(0.5, float(reconnect_delay_s))
        self._udp_transport: Optional[asyncio.DatagramTransport] = None
        self._udp_ready = asyncio.Event()
        self._tcp_reader: Optional[asyncio.StreamReader] = None
        self._tcp_writer: Optional[asyncio.StreamWriter] = None
        self._tcp_connected = asyncio.Event()
        self._tcp_send_lock = asyncio.Lock()
        self._tcp_task: Optional[asyncio.Task[None]] = None
        self._enabled = _telemetry_config_valid(
            enabled=enabled,
            protocol=self._protocol,
            host=self._host,
            port=self._port,
        )

    @property
    def enabled(self) -> bool:
        return self._enabled

    @property
    def protocol(self) -> str:
        return self._protocol

    def set_client_hooks(
        self,
        *,
        on_connected: Optional[OnClientEvent] = None,
        on_disconnected: Optional[OnClientEvent] = None,
    ) -> None:
        self._on_client_connected = on_connected
        self._on_client_disconnected = on_disconnected

    async def start(self) -> None:
        if not self._enabled:
            return
        if self._protocol == "udp":
            await self._start_udp()
            return
        self._tcp_task = asyncio.create_task(self._tcp_server_loop(), name="dt-rti-twoway-tcp")

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

    def _tcp_listen_host(self) -> str:
        return self._bind_address or "0.0.0.0"

    async def _tcp_server_loop(self) -> None:
        bind_host = self._tcp_listen_host()
        while self._enabled:
            server: Optional[asyncio.Server] = None
            try:
                server = await asyncio.start_server(
                    self._on_tcp_client,
                    host=bind_host,
                    port=self._port,
                    reuse_address=True,
                )
                sockets = server.sockets or []
                addrs = ", ".join(str(s.getsockname()) for s in sockets)
                LOG.info(
                    "RTI Two Way Strings TCP listening on %s (port %d)",
                    addrs or f"{bind_host}:{self._port}",
                    self._port,
                )
                async with server:
                    await server.serve_forever()
            except asyncio.CancelledError:
                raise
            except Exception:
                LOG.warning(
                    "RTI Two Way Strings TCP server ended (%s:%d)",
                    bind_host,
                    self._port,
                    exc_info=True,
                )
            finally:
                if server is not None:
                    server.close()
                    with contextlib.suppress(Exception):
                        await server.wait_closed()
                await self._drop_tcp_client()
            await asyncio.sleep(self._reconnect_delay_s)

    async def _on_tcp_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        peer = writer.get_extra_info("peername")
        LOG.info("RTI Two Way Strings TCP client connected from %s", peer)
        await self._set_tcp_client(reader, writer)
        if self._on_client_connected is not None:
            with contextlib.suppress(Exception):
                await self._on_client_connected()
        try:
            await self._tcp_read_loop(reader)
        except asyncio.CancelledError:
            raise
        except Exception:
            LOG.debug("RTI Two Way TCP read ended (%s)", peer, exc_info=True)
        finally:
            await self._drop_tcp_client(reader, writer)
            if self._on_client_disconnected is not None:
                with contextlib.suppress(Exception):
                    await self._on_client_disconnected()
            LOG.info("RTI Two Way Strings TCP client disconnected (%s)", peer)

    async def _set_tcp_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        async with self._tcp_send_lock:
            old_writer = self._tcp_writer
            self._tcp_reader = reader
            self._tcp_writer = writer
            self._tcp_connected.set()
            if old_writer is not None and old_writer is not writer:
                old_writer.close()
                with contextlib.suppress(Exception):
                    await old_writer.wait_closed()

    async def _drop_tcp_client(
        self,
        reader: Optional[asyncio.StreamReader] = None,
        writer: Optional[asyncio.StreamWriter] = None,
    ) -> None:
        async with self._tcp_send_lock:
            if reader is not None and self._tcp_reader is not reader:
                return
            if writer is not None and self._tcp_writer is not writer:
                return
            close_writer = self._tcp_writer
            self._tcp_reader = None
            self._tcp_writer = None
            self._tcp_connected.clear()
            if close_writer is not None:
                close_writer.close()
                with contextlib.suppress(Exception):
                    await close_writer.wait_closed()

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
        payload = message.rstrip("\r\n").encode("utf-8", errors="replace") + RTI_TWOWAY_LINE_END
        if self._protocol == "udp":
            await self._udp_ready.wait()
            if self._udp_transport is not None:
                self._udp_transport.sendto(payload, (self._host, self._port))
            return
        try:
            await asyncio.wait_for(self._tcp_connected.wait(), timeout=self._connect_timeout_s)
        except asyncio.TimeoutError:
            LOG.debug(
                "RTI Two Way TCP: no client connected; dropped status line (listen port %d)",
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

    async def send_lines(self, lines: list[str]) -> None:
        """Send multiple LF-terminated lines in one write (full RTI refresh)."""
        if not lines:
            return
        body = "\n".join(line.rstrip("\r\n") for line in lines) + "\n"
        await self.send(body)


def _telemetry_config_valid(*, enabled: bool, protocol: str, host: str, port: int) -> bool:
    if not enabled or port <= 0:
        return False
    if protocol == "udp":
        return bool(host)
    return True


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
