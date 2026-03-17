import argparse
import asyncio
import contextlib
import collections
import json
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


LOG = logging.getLogger("drivertranslator")

_LOG_RING: "collections.deque[str]" = collections.deque(maxlen=500)


class _RingBufferLogHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
        except Exception:
            msg = record.getMessage()
        _LOG_RING.append(msg)


async def _open_connection(
    host: str,
    port: int,
    *,
    timeout: float,
    local_addr: Optional[Tuple[str, int]] = None,
) -> Tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    """Open TCP connection, optionally binding to a specific local address (e.g. AVoIP NIC)."""
    if local_addr is None:
        return await asyncio.wait_for(
            asyncio.open_connection(host, port),
            timeout=timeout,
        )
    loop = asyncio.get_running_loop()
    reader = asyncio.StreamReader()
    protocol = asyncio.StreamReaderProtocol(reader)
    transport, _ = await asyncio.wait_for(
        loop.create_connection(lambda: protocol, host, port, local_addr=local_addr),
        timeout=timeout,
    )
    writer = asyncio.StreamWriter(transport, protocol, reader, loop)
    return reader, writer


def _crlf(line: str) -> bytes:
    return (line + "\r\n").encode("utf-8", errors="replace")


def _as_int(v: Any, *, default: int) -> int:
    try:
        return int(v)
    except Exception:
        return default


def _bind_addr(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def _as_bool(v: Any, *, default: bool) -> bool:
    if v is None:
        return default
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    if s in ("1", "true", "yes", "y", "on"):
        return True
    if s in ("0", "false", "no", "n", "off"):
        return False
    return default


@dataclass(frozen=True)
class Tx:
    alias: str
    hostname: str
    ip: Optional[str]
    amx_stream: int


@dataclass(frozen=True)
class Rx:
    alias: str
    hostname: str
    ip: Optional[str]
    amx_decoder_ip: str


@dataclass(frozen=True)
class NhdCtlIdentity:
    api: str
    web: str
    core: str
    ipsetting: Dict[str, str]
    ipsetting2: Dict[str, str]


@dataclass
class Config:
    nhd: NhdCtlIdentity
    tx_by_alias: Dict[str, Tx]
    tx_by_hostname: Dict[str, Tx]
    rx_by_alias: Dict[str, Rx]
    rx_by_hostname: Dict[str, Rx]
    amx_decoder_port: int
    amx_connect_timeout_ms: int
    amx_command_timeout_ms: int
    send_startup_notify_endpoint_online: bool
    amx_dry_run: bool
    amx_persistent: bool
    amx_keepalive_seconds: int
    amx_bind_address: Optional[str]
    rti_notify_enabled: bool
    rti_notify_protocol: str
    rti_notify_host: Optional[str]
    rti_notify_port: int
    rti_notify_bind_address: Optional[str]
    rti_notify_min_interval_seconds: int
    rti_notify_repeat_suppression_seconds: int
    rti_status_enabled: bool
    rti_status_protocol: str
    rti_status_host: Optional[str]
    rti_status_port: int
    rti_status_bind_address: Optional[str]
    rti_status_interval_seconds: int
    http_status_enabled: bool
    http_status_bind: str
    http_status_port: int
    http_status_log_lines: int


def load_config(path: str) -> Config:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))

    nhd_raw = raw.get("nhd_ctl", {})
    ver = nhd_raw.get("version", {})
    ip1 = nhd_raw.get("ipsetting", {})
    ip2 = nhd_raw.get("ipsetting2", {})

    nhd = NhdCtlIdentity(
        api=str(ver.get("api", "1.21")),
        web=str(ver.get("web", "8.3.1")),
        core=str(ver.get("core", "8.3.8")),
        ipsetting={
            "ip4addr": str(ip1.get("ip4addr", "169.254.1.1")),
            "netmask": str(ip1.get("netmask", "255.255.0.0")),
            "gateway": str(ip1.get("gateway", "169.254.1.254")),
        },
        ipsetting2={
            "ip4addr": str(ip2.get("ip4addr", "192.168.11.243")),
            "netmask": str(ip2.get("netmask", "255.255.255.0")),
            "gateway": str(ip2.get("gateway", "192.168.11.1")),
        },
    )

    endpoints = raw.get("endpoints", {})
    tx_list = endpoints.get("tx", [])
    rx_list = endpoints.get("rx", [])

    txs: List[Tx] = []
    for t in tx_list:
        alias = str(t["alias"])
        hostname = str(t.get("hostname") or f"NHD-TX-{alias}")
        ip = t.get("ip")
        ip_s = str(ip) if ip is not None else None
        amx_stream = _as_int(t.get("amx_stream"), default=0)
        txs.append(Tx(alias=alias, hostname=hostname, ip=ip_s, amx_stream=amx_stream))

    rxs: List[Rx] = []
    for r in rx_list:
        alias = str(r["alias"])
        hostname = str(r.get("hostname") or f"NHD-RX-{alias}")
        ip = r.get("ip")
        ip_s = str(ip) if ip is not None else None
        ip = str(r["amx_decoder_ip"])
        rxs.append(Rx(alias=alias, hostname=hostname, ip=ip_s, amx_decoder_ip=ip))

    amx = raw.get("amx", {})
    server = raw.get("server", {})
    rti_notify = raw.get("rti_notify", {})
    rti_status = raw.get("rti_status", {})
    http_status = raw.get("http_status", {})

    tx_by_alias = {t.alias: t for t in txs}
    tx_by_hostname = {t.hostname: t for t in txs}
    rx_by_alias = {r.alias: r for r in rxs}
    rx_by_hostname = {r.hostname: r for r in rxs}

    return Config(
        nhd=nhd,
        tx_by_alias=tx_by_alias,
        tx_by_hostname=tx_by_hostname,
        rx_by_alias=rx_by_alias,
        rx_by_hostname=rx_by_hostname,
        amx_decoder_port=_as_int(amx.get("decoder_port"), default=50002),
        amx_connect_timeout_ms=_as_int(amx.get("connect_timeout_ms"), default=1000),
        amx_command_timeout_ms=_as_int(amx.get("command_timeout_ms"), default=1500),
        send_startup_notify_endpoint_online=bool(
            server.get("send_startup_notify_endpoint_online", True)
        ),
        amx_dry_run=bool(amx.get("dry_run", False)),
        amx_persistent=bool(amx.get("persistent", False)),
        amx_keepalive_seconds=_as_int(amx.get("keepalive_seconds"), default=30),
        amx_bind_address=_bind_addr(amx.get("bind_address")),  # AVoIP NIC for outbound AMX
        rti_notify_enabled=_as_bool(rti_notify.get("enabled"), default=False),
        rti_notify_protocol=str(rti_notify.get("protocol", "udp")).strip().lower(),
        rti_notify_host=_bind_addr(rti_notify.get("host")),
        rti_notify_port=_as_int(rti_notify.get("port"), default=0),
        rti_notify_bind_address=_bind_addr(rti_notify.get("bind_address")),
        rti_notify_min_interval_seconds=_as_int(rti_notify.get("min_interval_seconds"), default=10),
        rti_notify_repeat_suppression_seconds=_as_int(
            rti_notify.get("repeat_suppression_seconds"), default=300
        ),
        rti_status_enabled=_as_bool(rti_status.get("enabled"), default=False),
        rti_status_protocol=str(rti_status.get("protocol", "udp")).strip().lower(),
        rti_status_host=_bind_addr(rti_status.get("host")),
        rti_status_port=_as_int(rti_status.get("port"), default=0),
        rti_status_bind_address=_bind_addr(rti_status.get("bind_address")),
        rti_status_interval_seconds=_as_int(rti_status.get("interval_seconds"), default=30),
        http_status_enabled=_as_bool(http_status.get("enabled"), default=True),
        http_status_bind=str(http_status.get("bind", "0.0.0.0")).strip() or "0.0.0.0",
        http_status_port=_as_int(http_status.get("port"), default=8080),
        http_status_log_lines=_as_int(http_status.get("log_lines"), default=200),
    )


class RtiNotifier:
    def __init__(
        self,
        *,
        enabled: bool,
        protocol: str,
        host: Optional[str],
        port: int,
        bind_address: Optional[str] = None,
        min_interval_seconds: int = 10,
        repeat_suppression_seconds: int = 300,
    ) -> None:
        self._enabled = enabled and bool(host) and int(port) > 0
        self._protocol = protocol
        self._host = host or ""
        self._port = int(port)
        self._bind_address = bind_address
        self._udp_transport: Optional[asyncio.DatagramTransport] = None
        self._udp_ready = asyncio.Event()
        self._lock = asyncio.Lock()
        self._min_interval = max(0, int(min_interval_seconds))
        self._repeat_suppression = max(0, int(repeat_suppression_seconds))
        self._last_sent_at: Dict[str, float] = {}
        self._last_sent_msg: Dict[str, str] = {}

    async def start(self) -> None:
        if not self._enabled:
            return
        if self._protocol == "udp":
            loop = asyncio.get_running_loop()
            local = (self._bind_address, 0) if self._bind_address else None
            transport, _ = await loop.create_datagram_endpoint(lambda: asyncio.DatagramProtocol(), local_addr=local)
            self._udp_transport = transport  # type: ignore[assignment]
            self._udp_ready.set()

    async def send(self, message: str) -> None:
        if not self._enabled:
            return
        msg = (message.rstrip("\r\n") + "\r\n").encode("utf-8", errors="replace")

        if self._protocol == "udp":
            await self._udp_ready.wait()
            if self._udp_transport is not None:
                self._udp_transport.sendto(msg, (self._host, self._port))
            return

        # TCP: connect, send, close (simple + robust)
        async with self._lock:
            try:
                reader, writer = await _open_connection(
                    self._host,
                    self._port,
                    timeout=1.5,
                    local_addr=(self._bind_address, 0) if self._bind_address else None,
                )
                writer.write(msg)
                await writer.drain()
                writer.close()
                with contextlib.suppress(Exception):
                    await writer.wait_closed()
            except Exception:
                LOG.debug("RTI notify send failed", exc_info=True)

    async def problem(self, key: str, message: str) -> None:
        """
        Problems-only notification with anti-spam:
        - per-key minimum interval
        - suppress identical messages for a longer window
        """
        if not self._enabled:
            return
        now = time.monotonic()
        last_at = self._last_sent_at.get(key)
        last_msg = self._last_sent_msg.get(key)

        msg = message.strip()
        if last_msg == msg and last_at is not None and (now - last_at) < self._repeat_suppression:
            return
        if last_at is not None and (now - last_at) < self._min_interval:
            return

        self._last_sent_at[key] = now
        self._last_sent_msg[key] = msg
        await self.send(msg)


class StatusReporter:
    def __init__(
        self,
        *,
        enabled: bool,
        protocol: str,
        host: Optional[str],
        port: int,
        bind_address: Optional[str],
        interval_seconds: int,
        health: HealthState,
        amx: Any,
        cfg: Config,
    ) -> None:
        self._enabled = enabled and bool(host) and int(port) > 0 and interval_seconds > 0
        self._interval = max(1, int(interval_seconds))
        self._health = health
        self._amx = amx
        self._cfg = cfg
        self._notifier = RtiNotifier(
            enabled=self._enabled,
            protocol=protocol,
            host=host,
            port=port,
            bind_address=bind_address,
            min_interval_seconds=0,
            repeat_suppression_seconds=0,
        )
        self._task: Optional[asyncio.Task[None]] = None

    async def start(self) -> None:
        if not self._enabled:
            return
        await self._notifier.start()
        self._task = asyncio.create_task(self._loop(), name="dt-status-reporter")

    async def _loop(self) -> None:
        while True:
            await asyncio.sleep(self._interval)
            await self._send_status()

    async def _send_status(self) -> None:
        mode = "dry_run" if self._cfg.amx_dry_run else ("persistent" if self._cfg.amx_persistent else "connect_close")

        amx_connected = 0
        amx_total_known = 0
        if hasattr(self._amx, "connection_summary"):
            try:
                amx_connected, amx_total_known = self._amx.connection_summary()
            except Exception:
                pass

        # Report configured endpoints as the "system size" baseline.
        tx_total = len(self._cfg.tx_by_alias)
        rx_total = len(self._cfg.rx_by_alias)

        msg = (
            f"DTSTATUS: mode={mode} rti_clients={self._health.rti_clients} "
            f"amx_connected={amx_connected}/{max(amx_total_known, rx_total)} "
            f"tx_total={tx_total} rx_total={rx_total}"
        )
        await self._notifier.send(msg)


def _http_response(status: str, content_type: str, body: bytes) -> bytes:
    headers = [
        f"HTTP/1.1 {status}",
        f"Content-Type: {content_type}",
        f"Content-Length: {len(body)}",
        "Connection: close",
        "",
        "",
    ]
    return "\r\n".join(headers).encode("ascii") + body


def _format_uptime(seconds: int) -> str:
    seconds = max(0, int(seconds))
    days, rem = divmod(seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, secs = divmod(rem, 60)
    parts: List[str] = []
    if days:
        parts.append(f"{days}d")
    if hours or days:
        parts.append(f"{hours}h")
    if minutes or hours or days:
        parts.append(f"{minutes}m")
    parts.append(f"{secs:02d}s")
    return " ".join(parts)


def _build_status_snapshot(*, cfg: Config, health: HealthState, amx: Any, started_at: float) -> Dict[str, Any]:
    now = time.monotonic()
    mode = "dry_run" if cfg.amx_dry_run else ("persistent" if cfg.amx_persistent else "connect_close")

    amx_connected = None
    amx_total_known = None
    if hasattr(amx, "connection_summary"):
        try:
            amx_connected, amx_total_known = amx.connection_summary()
        except Exception:
            amx_connected, amx_total_known = None, None

    uptime_seconds = int(now - started_at)
    return {
        "uptime_seconds": uptime_seconds,
        "mode": mode,
        "rti_clients": health.rti_clients,
        "tx_configured": len(cfg.tx_by_alias),
        "rx_configured": len(cfg.rx_by_alias),
        "amx_connected": amx_connected,
        "amx_total_known": amx_total_known,
    }

def _get_log_tail(n: int) -> List[str]:
    n = max(0, min(int(n), 500))
    if n == 0:
        return []
    return list(_LOG_RING)[-n:]


async def _handle_http_client(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    *,
    cfg: Config,
    health: HealthState,
    amx: Any,
    started_at: float,
) -> None:
    try:
        data = await asyncio.wait_for(reader.read(4096), timeout=2.0)
        if not data:
            return
        line = data.split(b"\r\n", 1)[0].decode("ascii", errors="replace")
        parts = line.split()
        if len(parts) < 2:
            writer.write(_http_response("400 Bad Request", "text/plain", b"bad request"))
            return
        method, path = parts[0], parts[1]
        if method != "GET":
            writer.write(_http_response("405 Method Not Allowed", "text/plain", b"method not allowed"))
            return

        snapshot = _build_status_snapshot(cfg=cfg, health=health, amx=amx, started_at=started_at)

        if path in ("/status", "/status.json"):
            body = (json.dumps(snapshot, indent=2) + "\n").encode("utf-8")
            writer.write(_http_response("200 OK", "application/json", body))
            return

        if path in ("/logs", "/logs.json"):
            body = (json.dumps({"lines": _get_log_tail(cfg.http_status_log_lines)}, indent=2) + "\n").encode(
                "utf-8"
            )
            writer.write(_http_response("200 OK", "application/json", body))
            return

        if path == "/" or path.startswith("/?"):
            uptime_h = _format_uptime(int(snapshot["uptime_seconds"]))
            amx_conn = (
                f"{snapshot['amx_connected']}/{max(snapshot['amx_total_known'] or 0, snapshot['rx_configured'])}"
                if snapshot["amx_connected"] is not None
                else "n/a"
            )
            log_lines = "\n".join(_get_log_tail(cfg.http_status_log_lines))
            body = f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>DriverTranslator Status</title>
  <style>
    :root {{
      color-scheme: light dark;
      --bg: #ffffff;
      --fg: #111111;
      --muted: #666666;
      --card: #ffffff;
      --border: #dddddd;
      --row: #f0f0f0;
      --code-bg: #f6f6f6;
      --code-fg: #111111;
      --log-bg: #0b1020;
      --log-fg: #e6e6e6;
      --link: #0b5fff;
    }}

    @media (prefers-color-scheme: dark) {{
      :root {{
        --bg: #0b0d12;
        --fg: #e9edf1;
        --muted: #a9b1ba;
        --card: #111521;
        --border: #223046;
        --row: #1a2233;
        --code-bg: #0f1420;
        --code-fg: #e9edf1;
        --log-bg: #0b1020;
        --log-fg: #e6e6e6;
        --link: #8ab4ff;
      }}
    }}

    [data-theme="light"] {{
      --bg: #ffffff;
      --fg: #111111;
      --muted: #666666;
      --card: #ffffff;
      --border: #dddddd;
      --row: #f0f0f0;
      --code-bg: #f6f6f6;
      --code-fg: #111111;
      --log-bg: #0b1020;
      --log-fg: #e6e6e6;
      --link: #0b5fff;
    }}

    [data-theme="dark"] {{
      --bg: #0b0d12;
      --fg: #e9edf1;
      --muted: #a9b1ba;
      --card: #111521;
      --border: #223046;
      --row: #1a2233;
      --code-bg: #0f1420;
      --code-fg: #e9edf1;
      --log-bg: #0b1020;
      --log-fg: #e6e6e6;
      --link: #8ab4ff;
    }}

    body {{
      font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif;
      margin: 24px;
      background: var(--bg);
      color: var(--fg);
    }}

    a {{ color: var(--link); }}

    .topbar {{
      max-width: 720px;
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 12px;
      margin-bottom: 12px;
    }}

    .btn {{
      border: 1px solid var(--border);
      background: var(--card);
      color: var(--fg);
      padding: 6px 10px;
      border-radius: 8px;
      cursor: pointer;
      font-size: 13px;
    }}

    .card {{ max-width: 720px; padding: 16px 18px; border: 1px solid var(--border); border-radius: 10px; background: var(--card); }}
    .row {{ display: flex; justify-content: space-between; padding: 8px 0; border-bottom: 1px solid var(--row); }}
    .row:last-child {{ border-bottom: 0; }}
    code {{ background: var(--code-bg); color: var(--code-fg); padding: 2px 6px; border-radius: 6px; }}
    pre {{ white-space: pre-wrap; background: var(--log-bg); color: var(--log-fg); padding: 12px; border-radius: 10px; overflow-x: auto; }}
    .subtle {{ color: var(--muted); font-size: 12px; }}
  </style>
</head>
<body>
  <div class="topbar">
    <h2 style="margin:0;">DriverTranslator Status</h2>
    <button class="btn" id="themeBtn" type="button">Toggle theme</button>
  </div>
  <div class="card">
    <div class="row"><div>Uptime</div><div><code>{uptime_h}</code></div></div>
    <div class="row"><div>Mode</div><div><code>{snapshot['mode']}</code></div></div>
    <div class="row"><div>RTI clients</div><div><code>{snapshot['rti_clients']}</code></div></div>
    <div class="row"><div>Configured TX</div><div><code>{snapshot['tx_configured']}</code></div></div>
    <div class="row"><div>Configured RX</div><div><code>{snapshot['rx_configured']}</code></div></div>
    <div class="row"><div>AMX connections (persistent)</div><div><code>{amx_conn}</code></div></div>
  </div>
  <p class="subtle">JSON: <a href="/status.json"><code>/status.json</code></a> • Logs JSON: <a href="/logs.json"><code>/logs.json</code></a></p>
  <h3>Recent logs</h3>
  <pre>{log_lines}</pre>
  <script>
    (function () {{
      const btn = document.getElementById('themeBtn');
      const root = document.documentElement;
      const key = 'dt_theme';

      function apply(theme) {{
        if (!theme) {{
          root.removeAttribute('data-theme');
          return;
        }}
        root.setAttribute('data-theme', theme);
      }}

      const saved = localStorage.getItem(key);
      if (saved === 'light' || saved === 'dark') apply(saved);

      btn.addEventListener('click', () => {{
        const current = root.getAttribute('data-theme');
        const next = current === 'dark' ? 'light' : 'dark';
        apply(next);
        localStorage.setItem(key, next);
      }});
    }})();
  </script>
</body>
</html>
""".encode("utf-8")
            writer.write(_http_response("200 OK", "text/html; charset=utf-8", body))
            return

        writer.write(_http_response("404 Not Found", "text/plain", b"not found"))
    finally:
        with contextlib.suppress(Exception):
            await writer.drain()
        writer.close()
        with contextlib.suppress(Exception):
            await writer.wait_closed()


class AmxClient:
    def __init__(
        self,
        *,
        decoder_port: int,
        connect_timeout_ms: int,
        command_timeout_ms: int,
        bind_address: Optional[str] = None,
    ):
        self._decoder_port = decoder_port
        self._connect_timeout = connect_timeout_ms / 1000
        self._command_timeout = command_timeout_ms / 1000
        self._local_addr: Optional[Tuple[str, int]] = (bind_address, 0) if bind_address else None
        self._locks: Dict[str, asyncio.Lock] = {}

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
            await self._set_stream_locked(decoder_ip=decoder_ip, stream=stream)

    async def _set_stream_locked(self, *, decoder_ip: str, stream: int) -> None:
        cmd = f"set:{stream}\r".encode("ascii")
        LOG.info("AMX -> %s:%d %r", decoder_ip, self._decoder_port, cmd)
        try:
            reader, writer = await _open_connection(
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
                await asyncio.wait_for(reader.read(256), timeout=self._command_timeout)
            except Exception:
                pass
        finally:
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()


class DryRunAmxClient:
    def __init__(self, *, decoder_port: int):
        self._decoder_port = decoder_port

    async def set_stream(self, *, decoder_ip: str, stream: int) -> None:
        if stream <= 0:
            raise ValueError(f"Invalid AMX stream id: {stream}")
        cmd = f"set:{stream}\\r".encode("ascii")
        LOG.info("AMX (dry-run) -> %s:%d %r", decoder_ip, self._decoder_port, cmd)


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
    ):
        self._decoder_port = decoder_port
        self._connect_timeout = connect_timeout_ms / 1000
        self._command_timeout = command_timeout_ms / 1000
        self._keepalive_seconds = max(0, int(keepalive_seconds))
        self._local_addr: Optional[Tuple[str, int]] = (bind_address, 0) if bind_address else None

        self._workers: Dict[str, "_DecoderWorker"] = {}
        self._workers_lock = asyncio.Lock()

    async def set_stream(self, *, decoder_ip: str, stream: int) -> None:
        if stream <= 0:
            raise ValueError(f"Invalid AMX stream id: {stream}")

        worker = await self._get_worker(decoder_ip)
        await worker.send(f"set:{stream}\r".encode("ascii"))

    async def _get_worker(self, decoder_ip: str) -> "_DecoderWorker":
        async with self._workers_lock:
            w = self._workers.get(decoder_ip)
            if w is None:
                w = _DecoderWorker(
                    decoder_ip=decoder_ip,
                    decoder_port=self._decoder_port,
                    connect_timeout=self._connect_timeout,
                    command_timeout=self._command_timeout,
                    keepalive_seconds=self._keepalive_seconds,
                    local_addr=self._local_addr,
                )
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


class _DecoderWorker:
    def __init__(
        self,
        *,
        decoder_ip: str,
        decoder_port: int,
        connect_timeout: float,
        command_timeout: float,
        keepalive_seconds: int,
        local_addr: Optional[Tuple[str, int]] = None,
    ) -> None:
        self._decoder_ip = decoder_ip
        self._decoder_port = decoder_port
        self._connect_timeout = connect_timeout
        self._command_timeout = command_timeout
        self._keepalive_seconds = keepalive_seconds
        self._local_addr = local_addr

        self._q: "asyncio.Queue[Tuple[bytes, asyncio.Future[None]]]" = asyncio.Queue()
        self._task: Optional[asyncio.Task[None]] = None

        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._connected_evt = asyncio.Event()
        self.is_connected: bool = False

    def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._run(), name=f"amx-worker:{self._decoder_ip}")

    async def send(self, payload: bytes) -> None:
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[None] = loop.create_future()
        await self._q.put((payload, fut))
        await fut

    async def _run(self) -> None:
        keepalive_task: Optional[asyncio.Task[None]] = None
        try:
            await self._ensure_connected()

            if self._keepalive_seconds > 0:
                keepalive_task = asyncio.create_task(self._keepalive_loop())

            while True:
                payload, fut = await self._q.get()
                try:
                    await self._send_with_retry(payload)
                    if not fut.done():
                        fut.set_result(None)
                except Exception as e:
                    if not fut.done():
                        fut.set_exception(e)
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

    async def _send_with_retry(self, payload: bytes) -> None:
        try:
            await self._send_raw(payload, log_label="AMX")
        except Exception:
            await self._reconnect()
            await self._send_raw(payload, log_label="AMX(retry)")

    async def _send_raw(self, payload: bytes, *, log_label: str) -> None:
        await self._ensure_connected()
        assert self._writer is not None
        assert self._reader is not None

        LOG.info("%s -> %s:%d %r", log_label, self._decoder_ip, self._decoder_port, payload)
        self._writer.write(payload)
        await self._writer.drain()

        # Best-effort read to keep RX buffers clear; don't block routing on large status packets.
        with contextlib.suppress(Exception):
            await asyncio.wait_for(self._reader.read(256), timeout=self._command_timeout)

    async def _ensure_connected(self) -> None:
        if self._writer is not None and not self._writer.is_closing():
            return
        await self._reconnect()

    async def _reconnect(self) -> None:
        await self._close()
        self._connected_evt.clear()
        self.is_connected = False

        try:
            reader, writer = await _open_connection(
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


class NhdCtlSession:
    def __init__(self) -> None:
        self.alias_mode: bool = True  # default per doc: on


class NhdState:
    """
    Shared state across sessions to emulate the controller.

    RTI drivers commonly rely on matrix query commands to populate feedback variables.
    """

    def __init__(self, cfg: Config) -> None:
        # NULL means "no assignment"
        self.video: Dict[str, Optional[str]] = {rx.alias: None for rx in cfg.rx_by_alias.values()}
        self.audio: Dict[str, Optional[str]] = {rx.alias: None for rx in cfg.rx_by_alias.values()}
        self.usb: Dict[str, Optional[str]] = {rx.alias: None for rx in cfg.rx_by_alias.values()}
        self.serial: Dict[str, Optional[str]] = {rx.alias: None for rx in cfg.rx_by_alias.values()}
        self.infrared: Dict[str, Optional[str]] = {rx.alias: None for rx in cfg.rx_by_alias.values()}

    def set_all_media(self, *, tx_alias: Optional[str], rx_aliases: List[str]) -> None:
        for rx in rx_aliases:
            self.video[rx] = tx_alias
            self.audio[rx] = tx_alias
            self.usb[rx] = tx_alias
            self.serial[rx] = tx_alias
            self.infrared[rx] = tx_alias

    def set_breakaway(self, *, kind: str, tx_alias: Optional[str], rx_aliases: List[str]) -> None:
        table = {
            "video": self.video,
            "audio": self.audio,
            "audio2": self.audio,  # treat as same for emulation purposes
            "usb": self.usb,
            "serial": self.serial,
            "infrared": self.infrared,
        }.get(kind)
        if table is None:
            return
        for rx in rx_aliases:
            table[rx] = tx_alias


class HealthState:
    def __init__(self) -> None:
        self.rti_clients: int = 0


def _lookup_tx(cfg: Config, token: str) -> Optional[Tx]:
    return cfg.tx_by_alias.get(token) or cfg.tx_by_hostname.get(token)


def _lookup_rx(cfg: Config, token: str) -> Optional[Rx]:
    return cfg.rx_by_alias.get(token) or cfg.rx_by_hostname.get(token)


def _all_endpoint_aliases(cfg: Config) -> List[str]:
    return list(cfg.tx_by_alias.keys()) + list(cfg.rx_by_alias.keys())


def _handle_config_get(cfg: Config, session: NhdCtlSession, cmd: str) -> List[str]:
    parts = cmd.split()
    if parts[:3] == ["config", "get", "version"]:
        return [
            f"API version: v{cfg.nhd.api}",
            f"System version: v{cfg.nhd.web}(v{cfg.nhd.core})",
        ]

    if parts[:3] == ["config", "get", "ipsetting"]:
        ip = cfg.nhd.ipsetting
        return [f"ipsetting is: ip4addr {ip['ip4addr']} netmask {ip['netmask']} gateway {ip['gateway']}"]

    if parts[:3] == ["config", "get", "ipsetting2"]:
        ip = cfg.nhd.ipsetting2
        return [f"ipsetting2 is: ip4addr {ip['ip4addr']} netmask {ip['netmask']} gateway {ip['gateway']}"]

    if parts[:3] == ["config", "get", "devicelist"]:
        # Doc: only online devices returned. We treat all configured devices as online.
        names = _all_endpoint_aliases(cfg)
        return ["devicelist is " + " ".join(names)]

    if parts[:3] == ["config", "get", "name"]:
        # `config get name` (all), or `config get name <aliasOrHostname>`
        if len(parts) == 3:
            lines: List[str] = []
            for t in cfg.tx_by_alias.values():
                lines.append(f"{t.hostname}'s alias is {t.alias}")
            for r in cfg.rx_by_alias.values():
                lines.append(f"{r.hostname}'s alias is {r.alias}")
            return lines

        token = parts[3]
        tx = _lookup_tx(cfg, token)
        if tx is not None:
            return [f"{tx.hostname}'s alias is {tx.alias}"]
        rx = _lookup_rx(cfg, token)
        if rx is not None:
            return [f"{rx.hostname}'s alias is {rx.alias}"]
        return ["unknown command"]

    if parts[:3] == ["config", "get", "devicejsonstring"]:
        # Minimal structure; real CTL returns much more (Appendix A).
        devices: List[Dict[str, Any]] = []
        for t in cfg.tx_by_alias.values():
            devices.append({"type": "TX", "alias": t.alias, "hostname": t.hostname})
        for r in cfg.rx_by_alias.values():
            devices.append({"type": "RX", "alias": r.alias, "hostname": r.hostname})
        return ["device json string: " + json.dumps({"devices": devices}, separators=(",", ":"))]

    if parts[:4] == ["config", "get", "device", "info"]:
        # `config get device info` (all), or `config get device info <TX|RX>`
        devices: List[Dict[str, Any]] = []
        if len(parts) == 4:
            for t in cfg.tx_by_alias.values():
                devices.append({"alias": t.alias, "hostname": t.hostname, "role": "TX"})
            for r in cfg.rx_by_alias.values():
                devices.append({"alias": r.alias, "hostname": r.hostname, "role": "RX"})
        else:
            token = parts[4]
            tx = _lookup_tx(cfg, token)
            if tx is not None:
                devices.append({"alias": tx.alias, "hostname": tx.hostname, "role": "TX"})
            else:
                rx = _lookup_rx(cfg, token)
                if rx is not None:
                    devices.append({"alias": rx.alias, "hostname": rx.hostname, "role": "RX"})
                else:
                    return ["unknown command"]
        return ["devices json info: " + json.dumps({"devices": devices}, separators=(",", ":"))]

    return ["unknown command"]


async def _handle_matrix_set(cfg: Config, amx: Any, cmd: str) -> Tuple[bool, str]:
    # cmd: "matrix set <TX> <RX1> <RX2> ... <RXn>"
    parts = cmd.split()
    if len(parts) < 4:
        return False, "unknown command"
    if parts[0].lower() != "matrix" or parts[1].lower() != "set":
        return False, "unknown command"

    tx_token = parts[2]
    rx_tokens = parts[3:]

    tx = _lookup_tx(cfg, tx_token)
    if tx is None:
        # Allow explicit NULL routing in WyreStorm API
        if tx_token.upper() == "NULL":
            tx = None
        else:
            return False, "unknown command"

    rxs: List[Rx] = []
    for token in rx_tokens:
        rx = _lookup_rx(cfg, token)
        if rx is None:
            return False, "unknown command"
        rxs.append(rx)

    if tx is not None:
        # Fire AMX commands (one per decoder). Do it concurrently.
        await asyncio.gather(
            *(amx.set_stream(decoder_ip=rx.amx_decoder_ip, stream=tx.amx_stream) for rx in rxs),
            return_exceptions=False,
        )
    else:
        LOG.info("AMX routing skipped: NULL assignment requested")

    # WyreStorm ack is a "command mirror"
    return True, cmd


def _format_matrix_info(
    *,
    heading: str,
    mapping: Dict[str, Optional[str]],
    rx_aliases: List[str],
) -> List[str]:
    lines = [f"{heading} information:"]
    for rx in rx_aliases:
        tx = mapping.get(rx)
        lines.append(f"{(tx if tx is not None else 'NULL')} {rx}")
    return lines


def _as_success(line: str) -> str:
    # Some WyreStorm API commands explicitly append success|failure, others just mirror.
    # Returning success for state-mutating commands keeps RTI drivers happy.
    if line.endswith(" success") or line.endswith(" failure"):
        return line
    return f"{line} success"


def _handle_multiview_get(cfg: Config, cmd: str) -> List[str]:
    parts = cmd.split()
    if parts[:2] == ["mscene", "get"]:
        # Minimal empty layout list response.
        # If RX specified, return a single "mscene list:" + that RX with no layouts.
        if len(parts) == 3:
            rx = _lookup_rx(cfg, parts[2])
            if rx is None:
                return ["unknown command"]
            return ["mscene list:", f"{rx.alias}"]
        # all RX
        lines = ["mscene list:"]
        for rx in cfg.rx_by_alias.values():
            lines.append(f"{rx.alias}")
        return lines

    if parts[:2] == ["mview", "get"]:
        # Minimal empty custom layout response.
        if len(parts) == 3:
            rx = _lookup_rx(cfg, parts[2])
            if rx is None:
                return ["unknown command"]
            return ["mview information:", f"{rx.alias} tile"]
        lines = ["mview information:"]
        for rx in cfg.rx_by_alias.values():
            lines.append(f"{rx.alias} tile")
        return lines

    return ["unknown command"]


def _handle_videowall_get(cfg: Config, cmd: str) -> List[str]:
    parts = cmd.split()
    if parts[:2] == ["scene", "get"]:
        return ["scene list:"]
    if parts[:2] == ["vw", "get"]:
        return ["Video wall information:"]
    if parts[:2] == ["wscene2", "get"]:
        return ["wscene2 list:"]
    return ["unknown command"]


async def handle_client(
    cfg: Config,
    amx: Any,
    state: NhdState,
    notifier: RtiNotifier,
    health: HealthState,
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
) -> None:
    peer = writer.get_extra_info("peername")
    LOG.info("RTI connected from %s", peer)
    session = NhdCtlSession()
    health.rti_clients += 1

    # Optional: push "endpoint online" notifications on connect (helps some drivers).
    if cfg.send_startup_notify_endpoint_online:
        for name in _all_endpoint_aliases(cfg):
            writer.write(_crlf(f"notify endpoint + {name}"))
        await writer.drain()

    try:
        while True:
            raw = await reader.readline()
            if not raw:
                break

            # NHD-CTL expects last delimiter to be LF; tolerate CRLF.
            line = raw.decode("utf-8", errors="replace").rstrip("\r\n")
            if not line:
                continue

            LOG.info("RTI -> %s", line)
            lower = line.lower()

            # Handle known commands.
            if lower.startswith("matrix set "):
                try:
                    ok, resp = await _handle_matrix_set(cfg, amx, line)
                    if ok:
                        parts = line.split()
                        tx_token = parts[2]
                        tx_alias: Optional[str]
                        if tx_token.upper() == "NULL":
                            tx_alias = None
                        else:
                            tx_obj = _lookup_tx(cfg, tx_token)
                            tx_alias = tx_obj.alias if tx_obj is not None else None

                        rx_aliases: List[str] = []
                        for tok in parts[3:]:
                            rx_obj = _lookup_rx(cfg, tok)
                            if rx_obj is not None:
                                rx_aliases.append(rx_obj.alias)
                        state.set_all_media(tx_alias=tx_alias, rx_aliases=rx_aliases)
                except Exception as e:
                    LOG.exception("AMX routing failed")
                    await notifier.problem("amx.route", f"DT: ERROR AMX route failed: {e}")
                    ok, resp = False, f"error {e}"
                writer.write(_crlf(resp if ok else "unknown command"))
                await writer.drain()
                continue

            # Breakaway switching
            if lower.startswith("matrix ") and " set " in lower:
                parts = line.split()
                # matrix <kind> set <TX|NULL> <RX...>
                if len(parts) >= 5 and parts[0].lower() == "matrix" and parts[2].lower() == "set":
                    kind = parts[1].lower()
                    tx_token = parts[3]
                    rx_tokens = parts[4:]

                    tx_obj = _lookup_tx(cfg, tx_token)
                    tx_alias = None
                    if tx_obj is None:
                        if tx_token.upper() != "NULL":
                            writer.write(_crlf("unknown command"))
                            await writer.drain()
                            continue
                    else:
                        tx_alias = tx_obj.alias

                    rx_aliases: List[str] = []
                    for tok in rx_tokens:
                        rx_obj = _lookup_rx(cfg, tok)
                        if rx_obj is None:
                            writer.write(_crlf("unknown command"))
                            await writer.drain()
                            break
                        rx_aliases.append(rx_obj.alias)
                    else:
                        state.set_breakaway(kind=kind, tx_alias=tx_alias, rx_aliases=rx_aliases)

                        # Only video breakaway affects AMX in our model
                        if kind == "video" and tx_obj is not None:
                            try:
                                # rx_aliases are already validated above; use direct map for safety.
                                await asyncio.gather(
                                    *(
                                        amx.set_stream(
                                            decoder_ip=cfg.rx_by_alias[a].amx_decoder_ip,
                                            stream=tx_obj.amx_stream,
                                        )
                                        for a in rx_aliases
                                    ),
                                    return_exceptions=False,
                                )
                            except Exception as e:
                                LOG.exception("AMX routing failed")
                                await notifier.problem(
                                    "amx.breakaway.video", f"DT: ERROR AMX breakaway video route failed: {e}"
                                )
                                writer.write(_crlf("unknown command"))
                                await writer.drain()
                                continue

                        writer.write(_crlf(line))  # command mirror ack
                        await writer.drain()
                        continue

            # Matrix query commands used for RTI feedback variables
            if lower.startswith("matrix ") and " get" in lower:
                parts = line.split()
                # Examples:
                # matrix video get [<RX...>]
                # matrix audio get [<RX...>]
                if len(parts) >= 3 and parts[0].lower() == "matrix" and parts[2].lower() == "get":
                    kind = parts[1].lower()
                    rx_tokens = parts[3:]
                    rx_aliases: List[str] = []
                    if rx_tokens:
                        for tok in rx_tokens:
                            rx_obj = _lookup_rx(cfg, tok)
                            if rx_obj is None:
                                writer.write(_crlf("unknown command"))
                                await writer.drain()
                                break
                            rx_aliases.append(rx_obj.alias)
                        else:
                            # all rx parsed ok
                            pass
                    else:
                        rx_aliases = list(cfg.rx_by_alias.keys())

                    table = {
                        "video": state.video,
                        "audio": state.audio,
                        "audio2": state.audio,
                        "usb": state.usb,
                        "serial": state.serial,
                        "infrared": state.infrared,
                    }.get(kind)
                    if table is None:
                        writer.write(_crlf("unknown command"))
                        await writer.drain()
                        continue

                    for resp_line in _format_matrix_info(
                        heading=f"matrix {kind}", mapping=table, rx_aliases=rx_aliases
                    ):
                        writer.write(_crlf(resp_line))
                    await writer.drain()
                    continue

            if lower.startswith("config set session alias "):
                parts = line.split()
                if len(parts) == 5 and parts[4].lower() in ("on", "off"):
                    session.alias_mode = parts[4].lower() == "on"
                    writer.write(_crlf(line))  # command mirror ack
                else:
                    writer.write(_crlf("unknown command"))
                await writer.drain()
                continue

            if lower.startswith("config set "):
                # For many config set commands, WyreStorm replies with command mirror.
                writer.write(_crlf(line))
                await writer.drain()
                continue

            if lower.startswith("config get "):
                for resp_line in _handle_config_get(cfg, session, line):
                    writer.write(_crlf(resp_line))
                await writer.drain()
                continue

            # Safe mirrors / minimal responses for RTI driver feature surface.
            # Video wall + multiview query commands (return empty lists rather than "unknown command")
            if lower.startswith(("scene get", "vw get", "wscene2 get")):
                for resp_line in _handle_videowall_get(cfg, line):
                    writer.write(_crlf(resp_line))
                await writer.drain()
                continue

            if lower.startswith(("mscene get", "mview get")):
                for resp_line in _handle_multiview_get(cfg, line):
                    writer.write(_crlf(resp_line))
                await writer.drain()
                continue

            # Scene/multiview activation and edits: acknowledge success.
            if lower.startswith(
                (
                    "scene active ",
                    "wscene2 active ",
                    "vw active ",
                    "mscene active ",
                    "mscene change ",
                    "mscene set ",
                    "mview set ",
                    "mview set audio ",
                    "cec ",
                    "infrared ",
                    "serial ",
                    "api ",
                )
            ):
                # Some of these commands have defined response structure with success|failure.
                if lower.startswith(("mscene active ", "mscene change ", "mscene set ", "mview set ", "mview set audio ")):
                    writer.write(_crlf(_as_success(line)))
                else:
                    writer.write(_crlf(line))
                await writer.drain()
                continue

            # Unknown command
            writer.write(_crlf("unknown command"))
            await writer.drain()

    finally:
        LOG.info("RTI disconnected from %s", peer)
        health.rti_clients = max(0, health.rti_clients - 1)
        writer.close()
        with contextlib.suppress(Exception):
            await writer.wait_closed()


async def run_server(*, cfg: Config, listen: str, port: int) -> None:
    started_at = time.monotonic()
    if cfg.amx_bind_address:
        LOG.info("AMX outbound connections will bind to %s (AVoIP NIC)", cfg.amx_bind_address)

    if cfg.amx_dry_run:
        LOG.warning("AMX dry-run enabled: no TCP connections will be made.")
        amx: Any = DryRunAmxClient(decoder_port=cfg.amx_decoder_port)
    elif cfg.amx_persistent:
        LOG.warning("AMX persistent mode enabled: keeping per-decoder sockets open.")
        amx = PersistentAmxClient(
            decoder_port=cfg.amx_decoder_port,
            connect_timeout_ms=cfg.amx_connect_timeout_ms,
            command_timeout_ms=cfg.amx_command_timeout_ms,
            keepalive_seconds=cfg.amx_keepalive_seconds,
            bind_address=cfg.amx_bind_address,
        )
    else:
        amx = AmxClient(
            decoder_port=cfg.amx_decoder_port,
            connect_timeout_ms=cfg.amx_connect_timeout_ms,
            command_timeout_ms=cfg.amx_command_timeout_ms,
            bind_address=cfg.amx_bind_address,
        )

    state = NhdState(cfg)
    health = HealthState()

    notifier = RtiNotifier(
        enabled=cfg.rti_notify_enabled,
        protocol=cfg.rti_notify_protocol,
        host=cfg.rti_notify_host,
        port=cfg.rti_notify_port,
        bind_address=cfg.rti_notify_bind_address,
        min_interval_seconds=cfg.rti_notify_min_interval_seconds,
        repeat_suppression_seconds=cfg.rti_notify_repeat_suppression_seconds,
    )
    await notifier.start()

    status = StatusReporter(
        enabled=cfg.rti_status_enabled,
        protocol=cfg.rti_status_protocol,
        host=cfg.rti_status_host,
        port=cfg.rti_status_port,
        bind_address=cfg.rti_status_bind_address,
        interval_seconds=cfg.rti_status_interval_seconds,
        health=health,
        amx=amx,
        cfg=cfg,
    )
    await status.start()

    if cfg.http_status_enabled:
        http_server = await asyncio.start_server(
            lambda r, w: _handle_http_client(r, w, cfg=cfg, health=health, amx=amx, started_at=started_at),
            host=cfg.http_status_bind,
            port=cfg.http_status_port,
        )
        addrs = ", ".join(str(sock.getsockname()) for sock in (http_server.sockets or []))
        LOG.info("HTTP status listening on %s", addrs)

    server = await asyncio.start_server(
        lambda r, w: handle_client(cfg, amx, state, notifier, health, r, w),
        host=listen,
        port=port,
    )

    addrs = ", ".join(str(sock.getsockname()) for sock in (server.sockets or []))
    LOG.info("Listening on %s", addrs)

    async with server:
        await server.serve_forever()


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="RTI -> WyreStorm NHD-CTL emulator -> AMX AVoIP translator")
    parser.add_argument("--config", required=True, help="Path to config.json")
    parser.add_argument("--listen", default="0.0.0.0", help="Address to bind (default 0.0.0.0)")
    parser.add_argument("--port", type=int, default=2323, help="TCP port to listen on (default 2323)")
    parser.add_argument("--log-level", default="INFO", help="DEBUG, INFO, WARNING, ERROR")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    ring = _RingBufferLogHandler()
    ring.setLevel(logging.DEBUG)
    ring.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
    logging.getLogger().addHandler(ring)

    cfg = load_config(args.config)

    try:
        asyncio.run(run_server(cfg=cfg, listen=args.listen, port=args.port))
    except KeyboardInterrupt:
        return 130
    return 0

