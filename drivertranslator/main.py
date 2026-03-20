from __future__ import annotations

import argparse
import asyncio
import contextlib
import collections
import html
import json
import logging
import random
import re
import secrets
import subprocess
import threading
import time
import urllib.parse
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


LOG = logging.getLogger("drivertranslator")

_LOG_RING: "collections.deque[str]" = collections.deque(maxlen=500)

# Deduplicated RTI lines that received "unknown command" (for status page / triage).
_UNKNOWN_CTL_MAX_KEYS = 400
_unknown_ctl: Dict[str, Dict[str, Any]] = {}
_unknown_ctl_lock = threading.Lock()
_unknown_ctl_file: Optional[Path] = None


def _unknown_ctl_configure(*, enabled: bool, config_dir: Path, persist_path: Optional[str]) -> None:
    global _unknown_ctl_file
    if not enabled:
        _unknown_ctl_file = None
        return
    if persist_path and str(persist_path).strip():
        _unknown_ctl_file = Path(persist_path).expanduser().resolve()
    else:
        _unknown_ctl_file = (config_dir / "unknown_ctl.json").resolve()


def _unknown_ctl_load_from_disk() -> None:
    global _unknown_ctl
    path = _unknown_ctl_file
    if path is None or not path.is_file():
        return
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        LOG.warning("unknown_ctl: could not load %s: %s", path, e)
        return
    entries = raw.get("entries") if isinstance(raw, dict) else None
    if not isinstance(entries, dict):
        return
    loaded: Dict[str, Dict[str, Any]] = {}
    for k, v in entries.items():
        if not isinstance(k, str) or not isinstance(v, dict):
            continue
        try:
            c = int(v.get("count", 1))
            first = str(v.get("first", ""))
            last = str(v.get("last", ""))
        except (TypeError, ValueError):
            continue
        if c < 1 or len(k) > 2000:
            continue
        loaded[k] = {"count": c, "first": first or "?", "last": last or "?"}
        if len(loaded) >= _UNKNOWN_CTL_MAX_KEYS:
            break
    with _unknown_ctl_lock:
        _unknown_ctl.clear()
        _unknown_ctl.update(loaded)
    LOG.info("unknown_ctl: loaded %d entr%s from %s", len(loaded), "y" if len(loaded) == 1 else "ies", path)


def _unknown_ctl_save_to_disk() -> None:
    path = _unknown_ctl_file
    if path is None:
        return
    with _unknown_ctl_lock:
        payload = {"v": 1, "entries": dict(_unknown_ctl)}
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(path.name + ".tmp")
        tmp.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        tmp.replace(path)
    except OSError as e:
        LOG.warning("unknown_ctl: save failed %s: %s", path, e)


def _unknown_ctl_clear_persisted() -> None:
    with _unknown_ctl_lock:
        _unknown_ctl.clear()
    _unknown_ctl_save_to_disk()


def _unknown_ctl_record(line: str) -> None:
    key = line.strip()
    if not key or len(key) > 2000:
        return
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    with _unknown_ctl_lock:
        if key in _unknown_ctl:
            e = _unknown_ctl[key]
            e["count"] = int(e["count"]) + 1
            e["last"] = now
        else:
            if len(_unknown_ctl) >= _UNKNOWN_CTL_MAX_KEYS:
                victim = min(
                    _unknown_ctl.items(),
                    key=lambda kv: (int(kv[1]["count"]), kv[1]["last"]),
                )[0]
                del _unknown_ctl[victim]
            _unknown_ctl[key] = {"count": 1, "first": now, "last": now}
    _unknown_ctl_save_to_disk()


def _unknown_ctl_page_text() -> str:
    with _unknown_ctl_lock:
        items = list(_unknown_ctl.items())
    if not items:
        return (
            "(No unrecognized commands yet.)\n\n"
            "When the WyreStorm/RTI driver sends a line the emulator does not handle, "
            "it appears here with a count."
        )
    items.sort(key=lambda x: (-int(x[1]["count"]), x[1]["last"]))
    lines = [
        "# DriverTranslator — unrecognized NHD-CTL / RTI TCP command lines",
        "# How to read this:",
        "#   - These are EXACT lines sent TO this service (RTI port, e.g. 2323).",
        "#   - Server replied: unknown command",
        "#   - COUNT = how many times that same line was sent (deduplicated).",
        "#   - Times are UTC. Copy from the dashed line down and paste into support chat.",
        "# ---------------------------------------------------------------------------",
        "",
    ]
    for cmd, meta in items:
        lines.append(
            f"{int(meta['count'])}× | first: {meta['first']} | last: {meta['last']}"
        )
        lines.append(f"    {cmd}")
        lines.append("")
    return "\n".join(lines)


# Short-lived tokens so the status page can call /control/* via fetch() (browsers do not send Basic Auth on fetch).
_HTTP_UI_SESS_TTL_SEC = 30 * 60
_HTTP_UI_SESS: Dict[str, float] = {}


def _http_parse_path_params(path: str) -> Tuple[str, Dict[str, str]]:
    if "?" not in path:
        return path, {}
    base, qs = path.split("?", 1)
    params: Dict[str, str] = {}
    for part in qs.split("&"):
        if not part or "=" not in part:
            continue
        k, v = part.split("=", 1)
        params[k] = urllib.parse.unquote_plus(v)
    return base, params


def _http_ui_sess_issue() -> str:
    now = time.time()
    for k, ts in list(_HTTP_UI_SESS.items()):
        if now - ts > _HTTP_UI_SESS_TTL_SEC:
            del _HTTP_UI_SESS[k]
    tok = secrets.token_urlsafe(24)
    _HTTP_UI_SESS[tok] = now
    return tok


def _http_ui_sess_valid(tok: str) -> bool:
    if not tok:
        return False
    ts = _HTTP_UI_SESS.get(tok)
    if ts is None:
        return False
    if time.time() - ts > _HTTP_UI_SESS_TTL_SEC:
        with contextlib.suppress(KeyError):
            del _HTTP_UI_SESS[tok]
        return False
    return True


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


def _opt_str(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    return s or None
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


def _clamp_int(v: Any, *, default: int, min_v: int, max_v: int) -> int:
    try:
        x = int(v)
    except Exception:
        x = default
    return max(min_v, min(max_v, x))


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
    amx_dry_run_offline_decoders: List[str]
    amx_verify_after_set: bool
    amx_verify_timeout_ms: int
    amx_set_queue_limit: int
    amx_self_test_on_start: bool
    amx_set_retry_attempts: int
    amx_set_retry_backoff_initial_ms: int
    amx_set_retry_backoff_max_ms: int
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
    http_status_control_token: Optional[str]
    http_status_password: str
    rti_control_enabled: bool
    rti_control_bind_address: Optional[str]
    rti_control_port: int
    rti_control_reboot_command: str
    unknown_ctl_enabled: bool
    unknown_ctl_persist_path: Optional[str]


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
    rti_control = raw.get("rti_control", {})
    unknown_ctl = raw.get("unknown_ctl") if isinstance(raw.get("unknown_ctl"), dict) else {}
    uc_pp = unknown_ctl.get("persist_path")
    uc_path_s = str(uc_pp).strip() if uc_pp is not None and str(uc_pp).strip() else None

    tx_by_alias = {t.alias: t for t in txs}
    tx_by_hostname = {t.hostname: t for t in txs}
    rx_by_alias = {r.alias: r for r in rxs}
    rx_by_hostname = {r.hostname: r for r in rxs}

    offline_decoders: List[str] = []
    od = amx.get("dry_run_offline_decoders", [])
    if isinstance(od, str):
        offline_decoders = [x.strip() for x in od.split(",") if x.strip()]
    elif isinstance(od, list):
        offline_decoders = [str(x).strip() for x in od if str(x).strip()]

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
        amx_dry_run_offline_decoders=offline_decoders,
        amx_verify_after_set=_as_bool(amx.get("verify_after_set"), default=True),
        amx_verify_timeout_ms=_clamp_int(amx.get("verify_timeout_ms"), default=800, min_v=100, max_v=5000),
        amx_set_queue_limit=_clamp_int(amx.get("set_queue_limit"), default=1, min_v=1, max_v=20),
        amx_self_test_on_start=_as_bool(amx.get("self_test_on_start"), default=True),
        amx_set_retry_attempts=_clamp_int(amx.get("set_retry_attempts"), default=3, min_v=1, max_v=10),
        amx_set_retry_backoff_initial_ms=_clamp_int(amx.get("set_retry_backoff_initial_ms"), default=200, min_v=0, max_v=5000),
        amx_set_retry_backoff_max_ms=_clamp_int(amx.get("set_retry_backoff_max_ms"), default=1200, min_v=0, max_v=10000),
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
        http_status_control_token=_opt_str(http_status.get("control_token")),
        http_status_password=str(http_status.get("password", "1234")),
        rti_control_enabled=_as_bool(rti_control.get("enabled"), default=False),
        rti_control_bind_address=_bind_addr(rti_control.get("bind_address")),
        rti_control_port=_as_int(rti_control.get("port"), default=0),
        rti_control_reboot_command=str(rti_control.get("reboot_command", "reboot")).strip() or "reboot",
        unknown_ctl_enabled=_as_bool(unknown_ctl.get("enabled"), default=True),
        unknown_ctl_persist_path=uc_path_s,
    )


def _retry_delay_seconds(*, attempt_index: int, initial_ms: int, max_ms: int) -> float:
    """
    attempt_index: 1..N (1 is first retry delay)
    Exponential backoff with small jitter.
    """
    if initial_ms <= 0 or max_ms <= 0:
        return 0.0
    base_ms = min(max_ms, int(initial_ms * (2 ** max(0, attempt_index - 1))))
    jitter_ms = int(base_ms * random.uniform(0.0, 0.2))
    return (base_ms + jitter_ms) / 1000.0


_RX_ALIAS_RE = re.compile(r"^OUT(\d+)\b", re.IGNORECASE)


def _rx_alias_sort_key(alias: str) -> Tuple[int, str]:
    """
    Natural sort for RX aliases like OUT1-TV1, OUT10-TV10, ...
    """
    m = _RX_ALIAS_RE.match(alias.strip())
    if not m:
        return (10**9, alias)
    try:
        return (int(m.group(1)), alias)
    except Exception:
        return (10**9, alias)


def _validate_config(cfg: Config) -> None:
    errors: List[str] = []

    if not cfg.tx_by_alias:
        errors.append("No TX endpoints configured.")
    if not cfg.rx_by_alias:
        errors.append("No RX endpoints configured.")

    # Aliases and streams
    for tx in cfg.tx_by_alias.values():
        if not tx.alias.upper().startswith("IN"):
            errors.append(f"TX alias does not start with IN: {tx.alias}")
        if tx.amx_stream <= 0:
            errors.append(f"TX has invalid amx_stream (must be > 0): {tx.alias} -> {tx.amx_stream}")

    for rx in cfg.rx_by_alias.values():
        if not rx.alias.upper().startswith("OUT"):
            errors.append(f"RX alias does not start with OUT: {rx.alias}")
        if not rx.amx_decoder_ip:
            errors.append(f"RX missing amx_decoder_ip: {rx.alias}")

    if cfg.amx_dry_run and cfg.amx_persistent:
        errors.append("Config invalid: amx.dry_run=true and amx.persistent=true cannot both be enabled.")

    if cfg.http_status_port <= 0 or cfg.http_status_port > 65535:
        errors.append(f"Invalid http_status.port: {cfg.http_status_port}")

    if errors:
        raise ValueError("Config validation failed:\n- " + "\n- ".join(errors))


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
        self._problems: Optional[ProblemState] = None

    def attach_problem_state(self, problems: ProblemState) -> None:
        self._problems = problems

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
        # Always log locally (and on the status page log tail).
        LOG.error("(rti_notify) %s", msg)
        if self._problems is not None:
            with contextlib.suppress(Exception):
                await self._problems.record(key=key, message=msg)
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
        runtime: RuntimeSettings,
    ) -> None:
        self._enabled = enabled and bool(host) and int(port) > 0 and interval_seconds > 0
        self._interval = max(1, int(interval_seconds))
        self._health = health
        self._amx = amx
        self._cfg = cfg
        self._runtime = runtime
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
            # Can be disabled at runtime via web controls.
            if self._runtime.rti_status_enabled:
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


def _parse_amx_status(data: bytes) -> Dict[str, str]:
    """
    AMX getStatus responses are \r-delimited key:value lines (per AMX direct control API).
    We parse a best-effort mapping for fields we care about (e.g. STREAM).
    """
    try:
        text = data.decode("utf-8", errors="replace")
    except Exception:
        return {}
    out: Dict[str, str] = {}
    for raw in text.replace("\n", "\r").split("\r"):
        line = raw.strip()
        if not line:
            continue
        if ":" in line:
            k, v = line.split(":", 1)
            out[k.strip().upper()] = v.strip()
    return out


async def _do_reboot(*, reason: str) -> None:
    LOG.error("REBOOT requested: %s", reason)
    await asyncio.sleep(1.0)
    try:
        subprocess.Popen(["/usr/bin/systemctl", "reboot"])
    except FileNotFoundError:
        subprocess.Popen(["systemctl", "reboot"])


async def _amx_self_test(*, cfg: Config, amx: Any) -> Dict[str, Any]:
    """
    Connectivity self-test: attempt to connect to each configured decoder IP.
    Returns a summary suitable for web UI and/or problem notification.
    """
    if cfg.amx_dry_run:
        return {"ok": len(cfg.rx_by_alias), "total": len(cfg.rx_by_alias), "unreachable": []}

    ok = 0
    unreachable: List[str] = []
    local_addr = (cfg.amx_bind_address, 0) if cfg.amx_bind_address else None

    for rx in cfg.rx_by_alias.values():
        ip = rx.amx_decoder_ip
        try:
            _r, w = await _open_connection(
                ip,
                cfg.amx_decoder_port,
                timeout=cfg.amx_connect_timeout_ms / 1000,
                local_addr=local_addr,
            )
            w.close()
            with contextlib.suppress(Exception):
                await w.wait_closed()
            ok += 1
        except Exception:
            unreachable.append(ip)

    return {"ok": ok, "total": len(cfg.rx_by_alias), "unreachable": unreachable}


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


def _params_want_html(params: Dict[str, str]) -> bool:
    return (params.get("html") or "").lower() in ("1", "true", "yes", "on")


def _control_feedback_html(
    *,
    ok: bool,
    headline: str,
    paragraphs: List[str],
    pre_json: Optional[Any] = None,
) -> bytes:
    status_cls = "banner-ok" if ok else "banner-bad"
    paras = "".join(f"<p class=\"detail\">{html.escape(p)}</p>" for p in paragraphs)
    pre_block = ""
    if pre_json is not None:
        pre_block = (
            "<p class=\"detail\"><b>Details (JSON)</b></p><pre>"
            + html.escape(json.dumps(pre_json, indent=2))
            + "</pre>"
        )
    page = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>{html.escape(headline)} — DriverTranslator</title>
  <style>
    :root {{ color-scheme: light dark; }}
    body {{
      font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif;
      margin: 0; min-height: 100vh;
      padding: 28px 20px 48px;
      background: #f4f6f9; color: #0f172a;
      line-height: 1.5;
    }}
    @media (prefers-color-scheme: dark) {{
      body {{ background: #0f1218; color: #e8eaef; }}
    }}
    .wrap {{ max-width: 560px; margin: 0 auto; }}
    .banner {{
      padding: 22px 24px; border-radius: 14px; margin-bottom: 20px;
      font-size: 1.2rem; font-weight: 700; letter-spacing: -0.02em;
      box-shadow: 0 1px 3px rgba(0,0,0,0.08);
    }}
    .banner-ok {{ background: linear-gradient(135deg, #dcfce7 0%, #bbf7d0 100%); color: #14532d; border: 1px solid #86efac; }}
    .banner-bad {{ background: linear-gradient(135deg, #fee2e2 0%, #fecaca 100%); color: #7f1d1d; border: 1px solid #fca5a5; }}
    @media (prefers-color-scheme: dark) {{
      .banner-ok {{ background: linear-gradient(135deg, #14532d 0%, #166534 100%); color: #bbf7d0; border-color: #22c55e; }}
      .banner-bad {{ background: linear-gradient(135deg, #7f1d1d 0%, #991b1b 100%); color: #fecaca; border-color: #ef4444; }}
    }}
    .detail {{ margin: 0 0 14px 0; color: #64748b; font-size: 15px; }}
    @media (prefers-color-scheme: dark) {{ .detail {{ color: #9aa3b2; }} }}
    pre {{
      background: #0f172a; color: #e2e8f0; padding: 16px 18px; border-radius: 12px;
      overflow-x: auto; font-size: 12px; line-height: 1.45; border: 1px solid #334155;
    }}
    a {{
      display: inline-block; margin-top: 8px; color: #2563eb; font-weight: 600;
      text-decoration: none; padding: 10px 0;
    }}
    a:hover {{ text-decoration: underline; }}
    @media (prefers-color-scheme: dark) {{ a {{ color: #7aa2ff; }} }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="banner {status_cls}">{html.escape(headline)}</div>
    {paras}
    {pre_block}
    <p><a href="/">← Back to status</a></p>
  </div>
</body>
</html>"""
    return page.encode("utf-8")


def _http_unauthorized() -> bytes:
    # Basic auth (password-only semantics; username ignored)
    headers = [
        "HTTP/1.1 401 Unauthorized",
        'WWW-Authenticate: Basic realm="DriverTranslator"',
        "Content-Type: text/plain",
        "Content-Length: 12",
        "Connection: close",
        "",
        "",
    ]
    return "\r\n".join(headers).encode("ascii") + b"unauthorized"


def _parse_basic_auth_password(data: bytes) -> Optional[str]:
    try:
        text = data.decode("iso-8859-1", errors="replace")
    except Exception:
        return None
    # Look for Authorization header in the initial read buffer
    for line in text.split("\r\n"):
        if line.lower().startswith("authorization:"):
            v = line.split(":", 1)[1].strip()
            if not v.lower().startswith("basic "):
                return None
            import base64

            b64 = v.split(None, 1)[1].strip()
            try:
                raw = base64.b64decode(b64).decode("utf-8", errors="replace")
            except Exception:
                return None
            # raw is "user:pass"
            if ":" in raw:
                return raw.split(":", 1)[1]
            return ""
    return None


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
    state: NhdState,
    runtime: RuntimeSettings,
    problems: ProblemState,
    started_at: float,
) -> None:
    try:
        try:
            data = await asyncio.wait_for(reader.read(4096), timeout=2.0)
        except asyncio.TimeoutError:
            # Common with port scans / health checks that connect but don't send a request.
            return
        if not data:
            return

        line = data.split(b"\r\n", 1)[0].decode("ascii", errors="replace")
        parts = line.split()
        if len(parts) < 2:
            writer.write(_http_response("400 Bad Request", "text/plain", b"bad request"))
            return
        method, path = parts[0], parts[1]
        path_only, early_params = _http_parse_path_params(path)
        control_via_ui = path_only.startswith("/control/") and _http_ui_sess_valid(
            early_params.get("ui_sess", "")
        )

        # Require Basic auth (except /control/* with valid ui_sess from this session's status page).
        if cfg.http_status_password:
            pw = _parse_basic_auth_password(data)
            if pw != cfg.http_status_password and not control_via_ui:
                writer.write(_http_unauthorized())
                return

        if method != "GET":
            writer.write(_http_response("405 Method Not Allowed", "text/plain", b"method not allowed"))
            return

        snapshot = _build_status_snapshot(cfg=cfg, health=health, amx=amx, started_at=started_at)
        rt = await runtime.snapshot()
        _ = await problems.snapshot()

        if path in ("/status", "/status.json"):
            body = (json.dumps(snapshot, indent=2) + "\n").encode("utf-8")
            writer.write(_http_response("200 OK", "application/json", body))
            return

        if path in ("/logs", "/logs.json"):
            body = (json.dumps({"lines": _get_log_tail(rt["http_log_lines"])}, indent=2) + "\n").encode(
                "utf-8"
            )
            writer.write(_http_response("200 OK", "application/json", body))
            return

        if path in ("/control", "/control.json"):
            body = (json.dumps(rt, indent=2) + "\n").encode("utf-8")
            writer.write(_http_response("200 OK", "application/json", body))
            return

        if path in ("/problems", "/problems.json"):
            prob = await problems.snapshot()
            body = (json.dumps({"problems": prob}, indent=2) + "\n").encode("utf-8")
            writer.write(_http_response("200 OK", "application/json", body))
            return

        # Basic control endpoints (optional token).
        # /control/set?key=<k>&value=<v>[&token=<t>]
        # /control/selftest?[token=<t>]
        # /control/reboot
        # /control/clear_unknown_ctl
        if path.startswith("/control/"):
            # very small query parsing (no urllib dependency)
            qs = ""
            if "?" in path:
                qs = path.split("?", 1)[1]
            params: Dict[str, str] = {}
            for part in qs.split("&"):
                if "=" in part:
                    k, v = part.split("=", 1)
                    params[k] = v

            token = cfg.http_status_control_token
            if token and params.get("token") != token:
                if _params_want_html(params):
                    writer.write(
                        _http_response(
                            "403 Forbidden",
                            "text/html; charset=utf-8",
                            _control_feedback_html(
                                ok=False,
                                headline="Not allowed",
                                paragraphs=[
                                    "Wrong or missing control token. "
                                    "Set it in config (http_status.control_token) and use the same value in the link."
                                ],
                            ),
                        )
                    )
                else:
                    writer.write(_http_response("403 Forbidden", "text/plain", b"forbidden"))
                return

            ctl_via = "status_page" if params.get("ui_sess") else "http_api"

            if path.startswith("/control/set"):
                key = params.get("key", "")
                value = params.get("value", "")
                want_html = _params_want_html(params)
                try:
                    if value.lower() in ("true", "1", "yes", "y", "on", "false", "0", "no", "n", "off"):
                        await runtime.set_bool(key, value.lower() in ("true", "1", "yes", "y", "on"))
                    else:
                        await runtime.set_int(key, int(value))
                except Exception:
                    LOG.warning(
                        "HTTP control [source=%s]: set rejected key=%r value=%r",
                        ctl_via,
                        key,
                        value,
                    )
                    bad_msg = (
                        "Expected key amx_verify_after_set or rti_status_enabled with true/false, "
                        "or amx_verify_timeout_ms with a number (100–5000)."
                    )
                    if want_html:
                        writer.write(
                            _http_response(
                                "400 Bad Request",
                                "text/html; charset=utf-8",
                                _control_feedback_html(
                                    ok=False,
                                    headline="Could not apply setting",
                                    paragraphs=[bad_msg],
                                ),
                            )
                        )
                    else:
                        writer.write(_http_response("400 Bad Request", "text/plain", b"bad control request"))
                    return
                snap = await runtime.snapshot()
                if want_html:
                    if key == "amx_verify_timeout_ms":
                        paras = [
                            f"amx_verify_timeout_ms is now {snap.get('amx_verify_timeout_ms')} ms.",
                            "Takes effect immediately; no service restart needed.",
                        ]
                    elif key == "amx_verify_after_set":
                        v = snap.get("amx_verify_after_set")
                        paras = [
                            f"amx_verify_after_set is now {str(v).lower()} (post-route AMX STREAM check).",
                            "Takes effect immediately; no service restart needed.",
                        ]
                    elif key == "rti_status_enabled":
                        v = snap.get("rti_status_enabled")
                        paras = [
                            f"rti_status_enabled is now {str(v).lower()} (UDP status heartbeat).",
                            "Takes effect immediately; no service restart needed.",
                        ]
                    else:
                        paras = [
                            f"Updated setting {key!r}.",
                            "Takes effect immediately; no service restart needed.",
                        ]
                    writer.write(
                        _http_response(
                            "200 OK",
                            "text/html; charset=utf-8",
                            _control_feedback_html(
                                ok=True,
                                headline="Saved",
                                paragraphs=paras,
                                pre_json={
                                    "amx_verify_after_set": snap.get("amx_verify_after_set"),
                                    "amx_verify_timeout_ms": snap.get("amx_verify_timeout_ms"),
                                    "rti_status_enabled": snap.get("rti_status_enabled"),
                                },
                            ),
                        )
                    )
                else:
                    body = (json.dumps(snap, indent=2) + "\n").encode("utf-8")
                    writer.write(_http_response("200 OK", "application/json", body))
                LOG.info(
                    "HTTP control [source=%s]: set %s=%r (verify_after_set=%s verify_timeout_ms=%s rti_status=%s)",
                    ctl_via,
                    key,
                    snap.get(key),
                    snap.get("amx_verify_after_set"),
                    snap.get("amx_verify_timeout_ms"),
                    snap.get("rti_status_enabled"),
                )
                return

            if path.startswith("/control/selftest"):
                res = await _amx_self_test(cfg=cfg, amx=amx)
                total = int(res.get("total") or 0)
                ok_n = int(res.get("ok") or 0)
                all_ok = total == 0 or ok_n == total
                unr = res.get("unreachable") or []
                LOG.info(
                    "HTTP control [source=%s]: self-test %s/%s decoders reachable%s",
                    ctl_via,
                    ok_n,
                    total,
                    f" unreachable={unr!r}" if unr else "",
                )
                if _params_want_html(params):
                    if total == 0:
                        paras = ["No RX / decoders are configured in this profile."]
                    else:
                        paras = [f"TCP connect to port {cfg.amx_decoder_port}: {ok_n} of {total} reachable."]
                        if unr:
                            paras.append("Unreachable: " + ", ".join(str(x) for x in unr))
                        else:
                            paras.append("All configured decoder IPs accepted a connection.")
                    writer.write(
                        _http_response(
                            "200 OK",
                            "text/html; charset=utf-8",
                            _control_feedback_html(
                                ok=all_ok,
                                headline="Self-test passed" if all_ok else "Self-test: issues found",
                                paragraphs=paras,
                                pre_json=res,
                            ),
                        )
                    )
                else:
                    body = (json.dumps(res, indent=2) + "\n").encode("utf-8")
                    writer.write(_http_response("200 OK", "application/json", body))
                return

            if path.startswith("/control/clear_unknown_ctl"):
                _unknown_ctl_clear_persisted()
                LOG.info("HTTP control [source=%s]: cleared unknown_ctl list", ctl_via)
                if _params_want_html(params):
                    writer.write(
                        _http_response(
                            "200 OK",
                            "text/html; charset=utf-8",
                            _control_feedback_html(
                                ok=True,
                                headline="List cleared",
                                paragraphs=[
                                    "The unrecognized-command list is empty.",
                                    "Reload the status page to refresh the box below.",
                                ],
                            ),
                        )
                    )
                else:
                    body = (json.dumps({"ok": True, "cleared": True}, indent=2) + "\n").encode("utf-8")
                    writer.write(_http_response("200 OK", "application/json", body))
                return

            if path.startswith("/control/reboot"):
                if _params_want_html(params):
                    writer.write(
                        _http_response(
                            "200 OK",
                            "text/html; charset=utf-8",
                            _control_feedback_html(
                                ok=True,
                                headline="Reboot scheduled",
                                paragraphs=[
                                    "The machine will restart shortly. This page and SSH will drop until the system is back.",
                                    "Open the status page again after boot if you need to confirm the service.",
                                ],
                            ),
                        )
                    )
                else:
                    body = (json.dumps({"ok": True, "action": "rebooting"}, indent=2) + "\n").encode("utf-8")
                    writer.write(_http_response("200 OK", "application/json", body))
                LOG.warning("HTTP control [source=%s]: host reboot requested", ctl_via)
                asyncio.create_task(_do_reboot(reason=f"http_control:{ctl_via}"))
                return

            writer.write(_http_response("404 Not Found", "text/plain", b"not found"))
            return

        if path == "/" or path.startswith("/?"):
            # Refresh per-RX HDMI output state from AMX status for page display.
            # In persistent mode, this is cheap and socket-safe.
            # In non-persistent mode, keep polling command-driven from RTI handlers.
            if cfg.amx_persistent:
                await _refresh_hdmi_outputs(
                    cfg=cfg,
                    amx=amx,
                    state=state,
                    timeout_ms=max(200, min(1500, int(runtime.amx_verify_timeout_ms))),
                )
            uptime_h = _format_uptime(int(snapshot["uptime_seconds"]))
            amx_conn = (
                f"{snapshot['amx_connected']}/{max(snapshot['amx_total_known'] or 0, snapshot['rx_configured'])}"
                if snapshot["amx_connected"] is not None
                else "n/a"
            )
            log_lines = "\n".join(_get_log_tail(rt["http_log_lines"]))
            route_rows = []
            for rx_alias in sorted(cfg.rx_by_alias.keys(), key=_rx_alias_sort_key):
                tx_alias = state.video.get(rx_alias) or "NULL"
                online = state.rx_online.get(rx_alias, True)
                status_txt = "ONLINE" if online else "OFFLINE"
                status_cls = "ok" if online else "bad"
                hdmi_enabled = state.rx_hdmi_output.get(rx_alias)
                if hdmi_enabled is True:
                    hdmi_txt = "ON"
                    hdmi_cls = "ok"
                elif hdmi_enabled is False:
                    hdmi_txt = "OFF"
                    hdmi_cls = "bad"
                else:
                    hdmi_txt = "UNKNOWN"
                    hdmi_cls = ""
                route_rows.append(
                    f"<tr><td><code>{rx_alias}</code></td><td><code>{tx_alias}</code></td><td class=\"{status_cls}\"><b>{status_txt}</b></td><td class=\"{hdmi_cls}\"><b>{hdmi_txt}</b></td></tr>"
                )
            route_html = "\n".join(route_rows)
            _ct = cfg.http_status_control_token or ""
            ctl_qs = ("&token=" + urllib.parse.quote_plus(_ct)) if _ct else ""
            _ui_sess = _http_ui_sess_issue()
            _ui_sess_js = json.dumps(_ui_sess)
            _ctl_qs_js = json.dumps(ctl_qs)
            _amx_port = int(cfg.amx_decoder_port)
            _unknown_pre = html.escape(_unknown_ctl_page_text())
            if _unknown_ctl_file is not None:
                _uc_persist_note = (
                    "Stored on disk at <code>"
                    + html.escape(str(_unknown_ctl_file))
                    + "</code> (survives restart). Use the button below the list to clear."
                )
            else:
                _uc_persist_note = (
                    "<b>Not persisted</b> (<code>unknown_ctl.enabled</code> is false in config); "
                    "restart clears this list. Set <code>unknown_ctl.enabled</code> to true to save beside config."
                )
            body = f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>DriverTranslator Status</title>
  <style>
    :root {{
      color-scheme: light dark;
      --accent: #2563eb;
      --accent-soft: rgba(37, 99, 235, 0.12);
      --bg: #f4f6f9;
      --fg: #0f172a;
      --muted: #64748b;
      --card: #ffffff;
      --border: #e2e8f0;
      --row: #f1f5f9;
      --code-bg: #f1f5f9;
      --code-fg: #0f172a;
      --log-bg: #0f172a;
      --log-fg: #e2e8f0;
      --link: #2563eb;
      --shadow: 0 1px 3px rgba(15, 23, 42, 0.06), 0 4px 14px rgba(15, 23, 42, 0.04);
    }}

    @media (prefers-color-scheme: dark) {{
      :root {{
        --accent: #60a5fa;
        --accent-soft: rgba(96, 165, 250, 0.12);
        --bg: #0c0f14;
        --fg: #f1f5f9;
        --muted: #94a3b8;
        --card: #141a24;
        --border: #273449;
        --row: #1a2332;
        --code-bg: #1e293b;
        --code-fg: #e2e8f0;
        --log-bg: #0a0e14;
        --log-fg: #cbd5e1;
        --link: #93c5fd;
        --shadow: 0 2px 8px rgba(0, 0, 0, 0.35);
      }}
    }}

    [data-theme="light"] {{
      --accent: #2563eb;
      --accent-soft: rgba(37, 99, 235, 0.12);
      --bg: #f4f6f9;
      --fg: #0f172a;
      --muted: #64748b;
      --card: #ffffff;
      --border: #e2e8f0;
      --row: #f1f5f9;
      --code-bg: #f1f5f9;
      --code-fg: #0f172a;
      --log-bg: #0f172a;
      --log-fg: #e2e8f0;
      --link: #2563eb;
      --shadow: 0 1px 3px rgba(15, 23, 42, 0.06), 0 4px 14px rgba(15, 23, 42, 0.04);
    }}

    [data-theme="dark"] {{
      --accent: #60a5fa;
      --accent-soft: rgba(96, 165, 250, 0.12);
      --bg: #0c0f14;
      --fg: #f1f5f9;
      --muted: #94a3b8;
      --card: #141a24;
      --border: #273449;
      --row: #1a2332;
      --code-bg: #1e293b;
      --code-fg: #e2e8f0;
      --log-bg: #0a0e14;
      --log-fg: #cbd5e1;
      --link: #93c5fd;
      --shadow: 0 2px 8px rgba(0, 0, 0, 0.35);
    }}

    * {{ box-sizing: border-box; }}
    body {{
      font-family: "Segoe UI", system-ui, -apple-system, Roboto, sans-serif;
      margin: 0;
      min-height: 100vh;
      background: var(--bg);
      color: var(--fg);
      line-height: 1.5;
    }}
    .wrap {{
      max-width: 820px;
      margin: 0 auto;
      padding: 28px 20px 48px;
    }}
    a {{ color: var(--link); text-decoration: none; }}
    a:hover {{ text-decoration: underline; }}

    .topbar {{
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 16px;
      margin-bottom: 28px;
      flex-wrap: wrap;
    }}
    .brand {{
      display: flex;
      flex-direction: column;
      gap: 2px;
    }}
    .brand h1 {{
      margin: 0;
      font-size: 1.45rem;
      font-weight: 700;
      letter-spacing: -0.02em;
    }}
    .brand span {{ font-size: 0.8rem; color: var(--muted); }}

    .btn {{
      border: 1px solid var(--border);
      background: var(--card);
      color: var(--fg);
      padding: 8px 14px;
      border-radius: 10px;
      cursor: pointer;
      font-size: 13px;
      font-weight: 500;
      box-shadow: var(--shadow);
      transition: transform 0.12s ease, border-color 0.12s;
    }}
    .btn:hover {{ border-color: var(--accent); }}
    .btn-primary {{
      background: var(--accent);
      color: #fff;
      border-color: transparent;
    }}
    .btn-primary:hover {{ filter: brightness(1.06); }}

    .card {{
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 14px;
      padding: 18px 20px;
      margin-bottom: 20px;
      box-shadow: var(--shadow);
    }}
    .card .row {{
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 12px;
      padding: 10px 0;
      border-bottom: 1px solid var(--border);
    }}
    .card .row:last-child {{ border-bottom: 0; }}

    .section-title {{
      font-size: 0.75rem;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      color: var(--muted);
      margin: 28px 0 10px 0;
      border-left: 3px solid var(--accent);
      padding-left: 10px;
    }}
    .section-title:first-of-type {{ margin-top: 0; }}

    code {{
      background: var(--code-bg);
      color: var(--code-fg);
      padding: 3px 8px;
      border-radius: 6px;
      font-size: 0.88em;
    }}
    .ok {{ color: #16a34a; font-weight: 600; }}
    .bad {{ color: #dc2626; font-weight: 600; }}

    .links-bar {{
      display: flex;
      flex-wrap: wrap;
      gap: 10px 16px;
      font-size: 12px;
      color: var(--muted);
      margin-bottom: 8px;
    }}
    .links-bar a {{ font-weight: 500; }}

    pre {{
      white-space: pre-wrap;
      background: var(--log-bg);
      color: var(--log-fg);
      padding: 16px 18px;
      border-radius: 12px;
      overflow-x: auto;
      font-size: 12px;
      line-height: 1.45;
      border: 1px solid var(--border);
    }}

    .table-wrap {{
      border-radius: 12px;
      border: 1px solid var(--border);
      overflow: hidden;
      box-shadow: var(--shadow);
      margin-bottom: 8px;
    }}
    table {{ border-collapse: collapse; width: 100%; background: var(--card); }}
    thead th {{
      background: var(--accent-soft);
      color: var(--fg);
      font-size: 11px;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      font-weight: 600;
      padding: 12px 14px;
      text-align: left;
      border-bottom: 1px solid var(--border);
    }}
    tbody tr {{ transition: background 0.1s; }}
    tbody tr:nth-child(even) {{ background: var(--row); }}
    tbody tr:hover {{ background: var(--accent-soft); }}
    tbody td {{ padding: 10px 14px; border-bottom: 1px solid var(--border); font-size: 14px; }}
    tbody tr:last-child td {{ border-bottom: 0; }}

    .subtle {{ color: var(--muted); font-size: 13px; line-height: 1.5; margin: 0 0 14px 0; max-width: 720px; }}

    .unk-ctl-actions {{
      margin: 14px 0 8px 0;
      padding-top: 12px;
      border-top: 1px solid var(--border);
    }}
    .unk-ctl-actions .ctrl-run {{
      padding: 10px 18px;
      border-radius: 10px;
      background: var(--accent);
      color: #fff;
      font-weight: 600;
      font-size: 14px;
      border: none;
      cursor: pointer;
      box-shadow: var(--shadow);
    }}
    .unk-ctl-actions .ctrl-run:hover {{ filter: brightness(1.06); }}
    .unk-ctl-actions .ctrl-run:disabled {{ opacity: 0.55; cursor: not-allowed; }}

    .help-icon {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      width: 18px;
      height: 18px;
      margin-left: 8px;
      font-size: 11px;
      font-weight: 700;
      border: 1px solid var(--border);
      border-radius: 50%;
      color: var(--muted);
      background: var(--row);
      cursor: help;
      vertical-align: middle;
      line-height: 1;
    }}
    .help-icon:hover {{ color: var(--accent); border-color: var(--accent); }}

    .ctrl-actions {{ display: flex; align-items: center; gap: 10px; flex-wrap: wrap; }}
    .ctrl-actions .ctrl-run {{
      padding: 6px 14px;
      border-radius: 8px;
      background: var(--accent-soft);
      color: var(--link);
      font-weight: 600;
      font-size: 13px;
      border: 1px solid var(--border);
      cursor: pointer;
    }}
    .ctrl-actions .ctrl-run:hover {{ filter: brightness(0.97); }}
    .ctrl-actions .ctrl-run:disabled {{ opacity: 0.5; cursor: not-allowed; }}

    .dt-modal[hidden] {{ display: none !important; }}
    .dt-modal:not([hidden]) {{
      position: fixed; inset: 0; z-index: 3000;
      display: flex; align-items: center; justify-content: center;
      padding: 20px;
    }}
    .dt-modal-backdrop {{
      position: absolute; inset: 0; background: rgba(15, 23, 42, 0.5);
      backdrop-filter: blur(2px);
    }}
    .dt-modal-card {{
      position: relative; max-width: 420px; width: 100%;
      padding: 26px 24px 22px; border-radius: 16px;
      background: var(--card); border: 1px solid var(--border);
      box-shadow: 0 20px 50px rgba(0,0,0,0.2);
    }}
    .dt-modal-card h2 {{ margin: 0 0 12px 0; font-size: 1.2rem; letter-spacing: -0.02em; }}
    .dt-modal-card p {{ margin: 0 0 20px 0; color: var(--muted); font-size: 14px; line-height: 1.55; }}
    .dt-modal-card.dt-ok h2 {{ color: #15803d; }}
    .dt-modal-card.dt-bad h2 {{ color: #b91c1c; }}
    [data-theme="dark"] .dt-modal-card.dt-ok h2 {{ color: #4ade80; }}
    [data-theme="dark"] .dt-modal-card.dt-bad h2 {{ color: #f87171; }}
  </style>
</head>
<body>
  <div class="wrap">
  <div class="topbar">
    <div class="brand">
      <h1>DriverTranslator</h1>
      <span>Status &amp; controls · full page reload every 5s while this tab is visible</span>
    </div>
    <button class="btn" id="themeBtn" type="button">Theme</button>
  </div>

  <div class="section-title">Overview</div>
  <div class="card">
    <div class="row"><div>Uptime</div><div><code>{uptime_h}</code></div></div>
    <div class="row"><div>Mode</div><div><code>{snapshot['mode']}</code></div></div>
    <div class="row"><div>RTI clients</div><div><code>{snapshot['rti_clients']}</code></div></div>
    <div class="row"><div>Configured TX</div><div><code>{snapshot['tx_configured']}</code></div></div>
    <div class="row"><div>Configured RX</div><div><code>{snapshot['rx_configured']}</code></div></div>
    <div class="row"><div>AMX connections</div><div><code>{amx_conn}</code></div></div>
  </div>

  <div class="links-bar">
    <a href="/status.json">status.json</a>
    <a href="/logs.json">logs.json</a>
    <a href="/control.json">control.json</a>
  </div>

  <div class="section-title">Controls</div>
  <p class="subtle">Runtime only (no config file). Hover <span class="help-icon" style="cursor:default" title="Each control has a ? with full help.">?</span> for details. Results open in a short on-page message.</p>
  <div class="card">
    <div class="row">
      <div><b>AMX verify after switch</b><span class="help-icon" title="When ON, after each route the translator asks each affected decoder for STREAM via AMX. Mismatches send rti_notify only; RTI still gets an immediate matrix ack.">?</span></div>
      <div class="ctrl-actions">
        <code id="st_amx_verify">{str(rt['amx_verify_after_set']).lower()}</code>
        <button type="button" class="ctrl-run" data-dt-ctl="set" data-key="amx_verify_after_set" data-value="{'false' if rt['amx_verify_after_set'] else 'true'}">Toggle</button>
      </div>
    </div>
    <div class="row">
      <div><b>AMX verify timeout</b><span class="help-icon" title="How long to wait (ms) for each decoder status read during verify. Lower is snappier; raise if decoders are slow or the network is busy.">?</span></div>
      <div class="ctrl-actions">
        <code id="st_verify_ms">{rt['amx_verify_timeout_ms']} ms</code>
        <input id="verifyTo" class="btn" style="width:88px; padding:6px 8px;" type="number" min="100" max="5000" step="50" value="{rt['amx_verify_timeout_ms']}"/>
        <button id="applyVerifyTo" class="btn btn-primary ctrl-run" type="button" data-dt-ctl="verify_ms">Apply</button>
      </div>
    </div>
    <div class="row">
      <div><b>RTI status heartbeat</b><span class="help-icon" title="When ON, DriverTranslator sends periodic DTSTATUS lines to your rti_status UDP target (if enabled in config). No service restart needed.">?</span></div>
      <div class="ctrl-actions">
        <code id="st_rti_status">{str(rt['rti_status_enabled']).lower()}</code>
        <button type="button" class="ctrl-run" data-dt-ctl="set" data-key="rti_status_enabled" data-value="{'false' if rt['rti_status_enabled'] else 'true'}">Toggle</button>
      </div>
    </div>
    <div class="row">
      <div><b>AMX self-test</b><span class="help-icon" title="Short TCP connect to every configured decoder IP. In dry-run mode all decoders count as reachable without connecting.">?</span></div>
      <div class="ctrl-actions"><button type="button" class="ctrl-run" data-dt-ctl="selftest">Run now</button></div>
    </div>
    <div class="row">
      <div><b>Reboot host</b><span class="help-icon" title="Reboots this Linux machine (systemctl reboot). SSH and this page drop until the system is back.">?</span></div>
      <div class="ctrl-actions"><button type="button" class="ctrl-run" data-dt-ctl="reboot">Reboot</button></div>
    </div>
  </div>

  <div class="section-title">Matrix</div>
  <p class="subtle">Emulated WyreStorm routing from RTI (<code>NULL</code> = no source). Status = last AMX send result per RX. HDMI Out = AMX <code>HDMIOFF</code> state (<code>ON</code>=enabled, <code>OFF</code>=disabled).</p>
  <div class="table-wrap">
  <table>
    <thead><tr><th>RX (Output)</th><th>TX (Input)</th><th>Status</th><th>HDMI Out</th></tr></thead>
    <tbody>
      {route_html}
    </tbody>
  </table>
  </div>

  <div class="section-title">Recent logs</div>
  <pre>{log_lines}</pre>

  <div class="section-title">Unrecognized RTI commands</div>
  <p class="subtle">Lines the WyreStorm driver sent on the <b>RTI TCP port</b> (e.g. 2323) that returned <code>unknown command</code> (or similar). Identical lines are merged; the number is how many times each was sent. Times are <b>UTC</b>. Select the box below and copy for support. {_uc_persist_note}</p>
  <pre id="unknownCtlPre" style="max-height:320px;overflow-y:auto;font-size:11px;">{_unknown_pre}</pre>
  <div class="unk-ctl-actions">
    <button type="button" class="ctrl-run" data-dt-ctl="copy_unknown_ctl">Copy all</button>
    <button type="button" class="ctrl-run" data-dt-ctl="clear_unknown_ctl">Clear unrecognized list</button>
    <span class="subtle" style="display:block;margin-top:8px;margin-bottom:0;">Wipes this list and the on-disk file (when persistence is enabled). Page reloads after confirm.</span>
  </div>

  <div id="dtModal" class="dt-modal" hidden>
    <div class="dt-modal-backdrop" id="dtModalBackdrop"></div>
    <div class="dt-modal-card" id="dtModalCard" role="dialog" aria-modal="true" aria-labelledby="dtModalTitle">
      <h2 id="dtModalTitle"></h2>
      <p id="dtModalText"></p>
      <button type="button" class="btn btn-primary" id="dtModalOk">OK</button>
    </div>
  </div>
  </div>
  <script>
    (function () {{
      const DT_UI = {{ sess: {_ui_sess_js}, ctl: {_ctl_qs_js}, port: {_amx_port} }};
      function ctlUrl(path) {{
        const sep = path.indexOf('?') >= 0 ? '&' : '?';
        let u = path + sep + 'ui_sess=' + encodeURIComponent(DT_UI.sess);
        if (DT_UI.ctl) u += DT_UI.ctl;
        return u;
      }}

      const modal = document.getElementById('dtModal');
      const modalCard = document.getElementById('dtModalCard');
      const modalTitle = document.getElementById('dtModalTitle');
      const modalText = document.getElementById('dtModalText');
      const modalOk = document.getElementById('dtModalOk');
      const modalBackdrop = document.getElementById('dtModalBackdrop');

      function showModal(ok, title, text) {{
        modalCard.classList.remove('dt-ok', 'dt-bad');
        modalCard.classList.add(ok ? 'dt-ok' : 'dt-bad');
        modalTitle.textContent = title;
        modalText.textContent = text;
        modal.removeAttribute('hidden');
      }}
      function hideModal() {{ modal.setAttribute('hidden', ''); }}
      modalOk.addEventListener('click', hideModal);
      modalBackdrop.addEventListener('click', hideModal);
      document.addEventListener('keydown', (e) => {{ if (e.key === 'Escape') hideModal(); }});

      const ctrlBtns = document.querySelectorAll('.ctrl-run');
      function setBusy(on) {{
        ctrlBtns.forEach((b) => {{ b.disabled = !!on; }});
      }}

      function setSavedMessage(key, j) {{
        if (key === 'amx_verify_timeout_ms')
          return 'Verify timeout is now ' + j.amx_verify_timeout_ms + ' ms. Applies immediately; no restart.';
        if (key === 'amx_verify_after_set')
          return 'AMX verify after switch is now ' + String(j.amx_verify_after_set).toLowerCase() + '. Applies immediately.';
        if (key === 'rti_status_enabled')
          return 'RTI status heartbeat is now ' + String(j.rti_status_enabled).toLowerCase() + '. Applies immediately.';
        return 'Setting updated. Applies immediately.';
      }}

      async function handleControlResponse(r, okTitle, getDetail) {{
        if (r.status === 401) {{
          showModal(false, 'Session expired', 'Refresh this page and sign in again, then retry.');
          return;
        }}
        if (r.status === 403) {{
          showModal(false, 'Not allowed', 'Wrong or missing control token in config. Check http_status.control_token.');
          return;
        }}
        const t = await r.text();
        let j = null;
        try {{ j = JSON.parse(t); }} catch (e) {{}}
        if (!r.ok) {{
          showModal(false, 'Failed', (j && j.error) ? j.error : (t.slice(0, 200) || r.status + ' ' + r.statusText));
          return;
        }}
        showModal(true, okTitle, getDetail(j, t));
      }}

      document.querySelectorAll('[data-dt-ctl="set"]').forEach((btn) => {{
        btn.addEventListener('click', async () => {{
          const key = btn.getAttribute('data-key');
          const value = btn.getAttribute('data-value');
          setBusy(true);
          try {{
            const url = ctlUrl('/control/set?key=' + encodeURIComponent(key) + '&value=' + encodeURIComponent(value));
            const r = await fetch(url);
            await handleControlResponse(r, 'Saved', (j) => {{
              if (!j) return 'Done.';
              const msg = setSavedMessage(key, j);
              if (key === 'amx_verify_after_set') {{
                const el = document.getElementById('st_amx_verify');
                if (el) el.textContent = String(j.amx_verify_after_set).toLowerCase();
                btn.setAttribute('data-value', j.amx_verify_after_set ? 'false' : 'true');
              }}
              if (key === 'rti_status_enabled') {{
                const el = document.getElementById('st_rti_status');
                if (el) el.textContent = String(j.rti_status_enabled).toLowerCase();
                btn.setAttribute('data-value', j.rti_status_enabled ? 'false' : 'true');
              }}
              return msg;
            }});
          }} catch (e) {{
            showModal(false, 'Network error', String(e.message || e));
          }} finally {{
            setBusy(false);
          }}
        }});
      }});

      const applyVerifyTo = document.getElementById('applyVerifyTo');
      const verifyTo = document.getElementById('verifyTo');
      if (applyVerifyTo && verifyTo) {{
        applyVerifyTo.addEventListener('click', async () => {{
          const v = String(Math.max(100, Math.min(5000, parseInt(verifyTo.value || '800', 10) || 800)));
          verifyTo.value = v;
          setBusy(true);
          try {{
            const r = await fetch(ctlUrl('/control/set?key=amx_verify_timeout_ms&value=' + encodeURIComponent(v)));
            await handleControlResponse(r, 'Saved', (j) => {{
              if (j && j.amx_verify_timeout_ms != null) {{
                const el = document.getElementById('st_verify_ms');
                if (el) el.textContent = j.amx_verify_timeout_ms + ' ms';
              }}
              return setSavedMessage('amx_verify_timeout_ms', j || {{}});
            }});
          }} catch (e) {{
            showModal(false, 'Network error', String(e.message || e));
          }} finally {{
            setBusy(false);
          }}
        }});
      }}

      document.querySelectorAll('[data-dt-ctl="selftest"]').forEach((btn) => {{
        btn.addEventListener('click', async () => {{
          setBusy(true);
          try {{
            const r = await fetch(ctlUrl('/control/selftest'));
            if (r.status === 401) {{
              showModal(false, 'Session expired', 'Refresh this page and sign in again, then retry.');
              return;
            }}
            if (r.status === 403) {{
              showModal(false, 'Not allowed', 'Wrong or missing control token in config.');
              return;
            }}
            const t = await r.text();
            let j = null;
            try {{ j = JSON.parse(t); }} catch (e) {{}}
            if (!r.ok) {{
              showModal(false, 'Self-test failed', (t || r.statusText).slice(0, 300));
              return;
            }}
            const total = (j && j.total) | 0, okn = (j && j.ok) | 0;
            const allOk = !total || okn === total;
            let detail = 'Done.';
            if (j) {{
              if (!total) detail = 'No decoders are configured.';
              else {{
                const unr = j.unreachable || [];
                if (unr.length)
                  detail = okn + ' of ' + total + ' decoders reachable on port ' + DT_UI.port + '. Unreachable: ' + unr.join(', ') + '.';
                else
                  detail = 'All ' + total + ' decoders accepted a TCP connection on port ' + DT_UI.port + '.';
              }}
            }}
            showModal(allOk, allOk ? 'Self-test passed' : 'Self-test: issues found', detail);
          }} catch (e) {{
            showModal(false, 'Network error', String(e.message || e));
          }} finally {{
            setBusy(false);
          }}
        }});
      }});

      document.querySelectorAll('[data-dt-ctl="clear_unknown_ctl"]').forEach((btn) => {{
        btn.addEventListener('click', async () => {{
          if (!confirm('Clear the unrecognized-command list? This cannot be undone.')) return;
          setBusy(true);
          try {{
            const r = await fetch(ctlUrl('/control/clear_unknown_ctl'));
            if (r.status === 401) {{
              showModal(false, 'Session expired', 'Refresh this page and sign in again, then retry.');
              return;
            }}
            if (r.status === 403) {{
              showModal(false, 'Not allowed', 'Wrong or missing control token in config.');
              return;
            }}
            const t = await r.text();
            if (!r.ok) {{
              let j = null;
              try {{ j = JSON.parse(t); }} catch (e) {{}}
              showModal(false, 'Failed', (j && j.error) ? j.error : (t.slice(0, 200) || r.statusText));
              return;
            }}
            location.reload();
          }} catch (e) {{
            showModal(false, 'Network error', String(e.message || e));
          }} finally {{
            setBusy(false);
          }}
        }});
      }});

      document.querySelectorAll('[data-dt-ctl="copy_unknown_ctl"]').forEach((btn) => {{
        btn.addEventListener('click', async () => {{
          const pre = document.getElementById('unknownCtlPre');
          if (!pre) {{
            showModal(false, 'Not found', 'Unknown command list box is missing.');
            return;
          }}
          const text = (pre.textContent || '').replace(/\u00a0/g, ' ').trimEnd();
          if (!text) {{
            showModal(true, 'Copied', 'List is empty.');
            return;
          }}
          try {{
            if (navigator.clipboard && window.isSecureContext) {{
              await navigator.clipboard.writeText(text);
              showModal(true, 'Copied', 'Copied full unrecognized-command list to clipboard.');
              return;
            }}
            // Fallback for HTTP/non-secure contexts where Clipboard API is blocked.
            const ta = document.createElement('textarea');
            ta.value = text;
            ta.setAttribute('readonly', '');
            ta.style.position = 'fixed';
            ta.style.left = '-9999px';
            ta.style.top = '0';
            document.body.appendChild(ta);
            ta.focus();
            ta.select();
            const ok = document.execCommand('copy');
            document.body.removeChild(ta);
            if (!ok) throw new Error('execCommand copy failed');
            showModal(true, 'Copied', 'Copied full unrecognized-command list to clipboard.');
          }} catch (e) {{
            showModal(false, 'Copy failed', 'Browser blocked clipboard access. Select the box and copy manually.');
          }}
        }});
      }});

      document.querySelectorAll('[data-dt-ctl="reboot"]').forEach((btn) => {{
        btn.addEventListener('click', async () => {{
          if (!confirm('Reboot this machine? SSH and this page will disconnect until it is back.')) return;
          setBusy(true);
          try {{
            const r = await fetch(ctlUrl('/control/reboot'));
            await handleControlResponse(r, 'Reboot scheduled', () =>
              'The system will restart shortly. Reopen this page after boot if needed.');
          }} catch (e) {{
            showModal(false, 'Network error', String(e.message || e));
          }} finally {{
            setBusy(false);
          }}
        }});
      }});

      const themeBtn = document.getElementById('themeBtn');
      const root = document.documentElement;
      const themeKey = 'dt_theme';
      function applyTheme(theme) {{
        if (!theme) {{ root.removeAttribute('data-theme'); return; }}
        root.setAttribute('data-theme', theme);
      }}
      const savedTheme = localStorage.getItem(themeKey);
      if (savedTheme === 'light' || savedTheme === 'dark') applyTheme(savedTheme);
      themeBtn.addEventListener('click', () => {{
        const current = root.getAttribute('data-theme');
        const next = current === 'dark' ? 'light' : 'dark';
        applyTheme(next);
        localStorage.setItem(themeKey, next);
      }});

      setInterval(function () {{
        if (!document.hidden) location.reload();
      }}, 5000);
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
        set_retry_attempts: int = 1,
        set_retry_backoff_initial_ms: int = 0,
        set_retry_backoff_max_ms: int = 0,
    ):
        self._decoder_port = decoder_port
        self._connect_timeout = connect_timeout_ms / 1000
        self._command_timeout = command_timeout_ms / 1000
        self._local_addr: Optional[Tuple[str, int]] = (bind_address, 0) if bind_address else None
        self._locks: Dict[str, asyncio.Lock] = {}
        self._set_retry_attempts = max(1, int(set_retry_attempts))
        self._set_retry_backoff_initial_ms = max(0, int(set_retry_backoff_initial_ms))
        self._set_retry_backoff_max_ms = max(0, int(set_retry_backoff_max_ms))

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

    async def verify_stream(self, *, decoder_ip: str, expected_stream: int, timeout_ms: int) -> bool:
        # Stateless client: open a connection and query status
        try:
            reader, writer = await _open_connection(
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
            parsed = _parse_amx_status(data)
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
                reader, writer = await _open_connection(
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
                parsed = _parse_amx_status(data)
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
            with contextlib.suppress(Exception):
                await asyncio.wait_for(reader.read(256), timeout=self._command_timeout)

            writer.write(b"?\r")
            await writer.drain()
            try:
                data = await asyncio.wait_for(reader.read(4096), timeout=timeout_ms / 1000)
            except Exception:
                return None
            parsed = _parse_amx_status(data)
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

    async def _send_locked(self, *, decoder_ip: str, cmd: bytes) -> None:
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
                delay = _retry_delay_seconds(
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
                delay = _retry_delay_seconds(
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
                delay = _retry_delay_seconds(
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
                delay = _retry_delay_seconds(
                    attempt_index=attempt,
                    initial_ms=self._set_retry_backoff_initial_ms,
                    max_ms=self._set_retry_backoff_max_ms,
                )
                if delay > 0:
                    await asyncio.sleep(delay)
        if last_exc is not None:
            raise last_exc
        return None


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

        self._workers: Dict[str, "_DecoderWorker"] = {}
        self._workers_lock = asyncio.Lock()

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
                    set_retry_attempts=self._set_retry_attempts,
                    set_retry_backoff_initial_ms=self._set_retry_backoff_initial_ms,
                    set_retry_backoff_max_ms=self._set_retry_backoff_max_ms,
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
        set_retry_attempts: int = 1,
        set_retry_backoff_initial_ms: int = 0,
        set_retry_backoff_max_ms: int = 0,
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

        self._cond = asyncio.Condition()
        self._set_pending: Optional[Tuple[int, asyncio.Future[None]]] = None
        self._task: Optional[asyncio.Task[None]] = None

        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._connected_evt = asyncio.Event()
        self.is_connected: bool = False
        self._op_lock = asyncio.Lock()

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
        parsed = _parse_amx_status(data)
        return parsed.get("STREAM") == str(expected_stream)

    async def get_hdmi_output(self, *, timeout_ms: int) -> Optional[bool]:
        try:
            data = await self._query_status(timeout_ms=timeout_ms)
        except Exception:
            return None
        parsed = _parse_amx_status(data)
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
                delay = _retry_delay_seconds(
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
        payload = (command if command.endswith("\r") else (command + "\r")).encode("ascii")
        async with self._op_lock:
            await self._ensure_connected()
            assert self._writer is not None
            assert self._reader is not None
            LOG.info("AMX -> %s:%d %r", self._decoder_ip, self._decoder_port, payload)
            self._writer.write(payload)
            await self._writer.drain()
            with contextlib.suppress(Exception):
                await asyncio.wait_for(self._reader.read(256), timeout=self._command_timeout)
            self._writer.write(b"?\r")
            await self._writer.drain()
            try:
                data = await asyncio.wait_for(self._reader.read(4096), timeout=timeout_ms / 1000)
            except Exception:
                return None
        parsed = _parse_amx_status(data)
        hdmi_off = (parsed.get("HDMIOFF") or "").strip().lower()
        if hdmi_off == "on":
            return False
        if hdmi_off == "off":
            return True
        return None

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

                        delay = _retry_delay_seconds(
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
                await asyncio.wait_for(self._reader.read(256), timeout=self._command_timeout)

    async def _query_status(self, *, timeout_ms: int) -> bytes:
        async with self._op_lock:
            await self._ensure_connected()
            assert self._writer is not None
            assert self._reader is not None
            self._writer.write(b"?\r")
            await self._writer.drain()
            try:
                return await asyncio.wait_for(self._reader.read(4096), timeout=timeout_ms / 1000)
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
        # Best-effort health status (updated on AMX send success/failure).
        self.rx_online: Dict[str, bool] = {rx.alias: True for rx in cfg.rx_by_alias.values()}
        # Best-effort HDMI output state from AMX status (True=on, False=off, None=unknown).
        self.rx_hdmi_output: Dict[str, Optional[bool]] = {rx.alias: None for rx in cfg.rx_by_alias.values()}

    def set_rx_online(self, rx_alias: str, online: bool) -> None:
        if rx_alias in self.rx_online:
            self.rx_online[rx_alias] = bool(online)

    def set_rx_hdmi_output(self, rx_alias: str, enabled: Optional[bool]) -> None:
        if rx_alias in self.rx_hdmi_output:
            self.rx_hdmi_output[rx_alias] = enabled

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


class ProblemState:
    def __init__(self, *, max_lines: int = 50) -> None:
        self._max = max(1, int(max_lines))
        self._items: collections.deque[Dict[str, Any]] = collections.deque(maxlen=self._max)
        self._lock = asyncio.Lock()

    async def record(self, *, key: str, message: str) -> None:
        async with self._lock:
            self._items.append(
                {"ts": int(time.time()), "key": key, "message": message.strip()}
            )

    async def snapshot(self) -> List[Dict[str, Any]]:
        async with self._lock:
            return list(self._items)


class RuntimeSettings:
    def __init__(self, cfg: Config) -> None:
        self._lock = asyncio.Lock()
        self.amx_verify_after_set: bool = cfg.amx_verify_after_set
        self.amx_verify_timeout_ms: int = cfg.amx_verify_timeout_ms
        self.amx_self_test_on_start: bool = cfg.amx_self_test_on_start
        self.rti_status_enabled: bool = cfg.rti_status_enabled
        self.http_log_lines: int = cfg.http_status_log_lines

    async def snapshot(self) -> Dict[str, Any]:
        async with self._lock:
            return {
                "amx_verify_after_set": self.amx_verify_after_set,
                "amx_verify_timeout_ms": self.amx_verify_timeout_ms,
                "amx_self_test_on_start": self.amx_self_test_on_start,
                "rti_status_enabled": self.rti_status_enabled,
                "http_log_lines": self.http_log_lines,
            }

    async def set_bool(self, key: str, value: bool) -> None:
        async with self._lock:
            if key == "amx_verify_after_set":
                self.amx_verify_after_set = value
            elif key == "amx_self_test_on_start":
                self.amx_self_test_on_start = value
            elif key == "rti_status_enabled":
                self.rti_status_enabled = value
            else:
                raise KeyError(key)

    async def set_int(self, key: str, value: int) -> None:
        async with self._lock:
            if key == "amx_verify_timeout_ms":
                self.amx_verify_timeout_ms = _clamp_int(value, default=800, min_v=100, max_v=5000)
            elif key == "http_log_lines":
                self.http_log_lines = _clamp_int(value, default=200, min_v=0, max_v=500)
            else:
                raise KeyError(key)


class _RtiControlUdp(asyncio.DatagramProtocol):
    def __init__(self, *, cfg: Config):
        self._cfg = cfg
        self._last_reboot_at = 0.0

    def datagram_received(self, data: bytes, addr) -> None:  # type: ignore[override]
        if not self._cfg.rti_control_enabled:
            return
        text = data.decode("utf-8", errors="replace").strip()
        if not text:
            return
        want = (self._cfg.rti_control_reboot_command or "reboot").strip()
        if text.lower() != want.lower():
            return

        now = time.monotonic()
        if (now - self._last_reboot_at) < 60:
            LOG.warning("RTI control reboot suppressed (cooldown)")
            return
        self._last_reboot_at = now
        LOG.error("RTI control requested reboot from %s", addr)
        asyncio.create_task(_do_reboot(reason=f"rti_udp:{addr}"))


def _lookup_tx(cfg: Config, token: str) -> Optional[Tx]:
    return cfg.tx_by_alias.get(token) or cfg.tx_by_hostname.get(token)


def _lookup_rx(cfg: Config, token: str) -> Optional[Rx]:
    return cfg.rx_by_alias.get(token) or cfg.rx_by_hostname.get(token)


def _all_endpoint_aliases(cfg: Config) -> List[str]:
    return list(cfg.tx_by_alias.keys()) + list(cfg.rx_by_alias.keys())


def _emulated_multicast_ips(*, stream_id: int) -> Tuple[str, str]:
    """Stable fake multicast addresses for TX status (100/200-series style API)."""
    s = max(0, int(stream_id))
    v = 16 + (s % 200)
    a = 40 + (s * 7 % 200)
    return (f"224.{v}.{a}.{200 + (s % 55)}", f"224.{v + 32}.{a}.{200 + (s % 55)}")


def _device_status_tx_dict(tx: Tx) -> Dict[str, str]:
    vid, aud = _emulated_multicast_ips(stream_id=tx.amx_stream)
    return {
        "aliasname": tx.alias,
        "audio stream ip address": aud,
        "encoding enable": "true",
        "hdmi in active": "true",
        "hdmi in frame rate": "60",
        "line out audio enable": "false",
        "name": tx.hostname,
        "resolution": "1920x1080",
        "stream frame rate": "60",
        "stream resolution": "1920x1080",
        "video stream ip address": vid,
    }


def _device_status_rx_dict(rx: Rx, state: NhdState) -> Dict[str, str]:
    online = bool(state.rx_online.get(rx.alias, True))
    hdmi_enabled = state.rx_hdmi_output.get(rx.alias)
    routed_tx = state.video.get(rx.alias)
    hdmi_out_active = "true" if hdmi_enabled is not False else "false"
    if online and routed_tx:
        return {
            "aliasname": rx.alias,
            "audio bitrate": "3072000",
            "audio input format": "lpcm",
            "hdcp status": "hdcp22",
            "hdmi out active": hdmi_out_active,
            "hdmi out audio enable": "true",
            "hdmi out frame rate": "60",
            "hdmi out resolution": "1920x1080",
            "line out audio enable": "true",
            "name": rx.hostname,
            "stream error count": "0",
            "stream frame rate": "60",
            "stream resolution": "1920x1080",
        }
    if online:
        return {
            "aliasname": rx.alias,
            "audio bitrate": "0",
            "audio input format": "lpcm",
            "hdcp status": "none",
            "hdmi out active": hdmi_out_active,
            "hdmi out audio enable": "false",
            "hdmi out frame rate": "0",
            "hdmi out resolution": "unknown",
            "line out audio enable": "false",
            "name": rx.hostname,
            "stream error count": "0",
            "stream frame rate": "0",
            "stream resolution": "unknown",
        }
    return {
        "aliasname": rx.alias,
        "audio bitrate": "0",
        "audio input format": "unknown",
        "hdcp status": "none",
        "hdmi out active": "false",
        "hdmi out audio enable": "false",
        "hdmi out frame rate": "0",
        "hdmi out resolution": "unknown",
        "line out audio enable": "false",
        "name": rx.hostname,
        "stream error count": "65535",
        "stream frame rate": "0",
        "stream resolution": "unknown",
    }


def _handle_config_get(cfg: Config, session: NhdCtlSession, state: NhdState, cmd: str) -> List[str]:
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

    # Section 13.2 — device real-time status (100/110/140/200-tier JSON shape, API v6.6 / Appendix-style).
    if parts[:4] == ["config", "get", "device", "status"]:
        tok = parts[4:]

        def _pack(rows: List[Dict[str, str]]) -> List[str]:
            body = {"devices status": rows}
            return ["devices status info:", json.dumps(body, separators=(",", ":"))]

        if len(tok) == 0:
            rows: List[Dict[str, str]] = []
            for t in cfg.tx_by_alias.values():
                rows.append(_device_status_tx_dict(t))
            for r in cfg.rx_by_alias.values():
                rows.append(_device_status_rx_dict(r, state))
            return _pack(rows)

        if len(tok) == 1:
            tx = _lookup_tx(cfg, tok[0])
            if tx is not None:
                return _pack([_device_status_tx_dict(tx)])
            rx = _lookup_rx(cfg, tok[0])
            if rx is not None:
                return _pack([_device_status_rx_dict(rx, state)])
            return ["unknown command"]

        if len(tok) == 2:
            a, b = tok[0], tok[1]
            tx_a, rx_a = _lookup_tx(cfg, a), _lookup_rx(cfg, a)
            tx_b, rx_b = _lookup_tx(cfg, b), _lookup_rx(cfg, b)
            tx: Optional[Tx] = None
            rx: Optional[Rx] = None
            if tx_a and rx_b:
                tx, rx = tx_a, rx_b
            elif tx_b and rx_a:
                tx, rx = tx_b, rx_a
            elif rx_a and tx_b:
                tx, rx = tx_b, rx_a
            elif rx_b and tx_a:
                tx, rx = tx_a, rx_b
            if tx is None or rx is None:
                return ["unknown command"]
            # API example order: TX (source) object first, then RX (display).
            return _pack([_device_status_tx_dict(tx), _device_status_rx_dict(rx, state)])

        return ["unknown command"]

    return ["unknown command"]


async def _handle_matrix_set(
    cfg: Config, amx: Any, state: NhdState, cmd: str, timeout_ms: int
) -> Tuple[bool, str, List[Tuple[str, str, str]]]:
    # cmd: "matrix set <TX> <RX1> <RX2> ... <RXn>"
    parts = cmd.split()
    if len(parts) < 4:
        return False, "unknown command", []
    if parts[0].lower() != "matrix" or parts[1].lower() != "set":
        return False, "unknown command", []

    tx_token = parts[2]
    rx_tokens = parts[3:]

    tx = _lookup_tx(cfg, tx_token)
    if tx is None:
        # Allow explicit NULL routing in WyreStorm API
        if tx_token.upper() == "NULL":
            tx = None
        else:
            return False, "unknown command", []

    rxs: List[Rx] = []
    for token in rx_tokens:
        rx = _lookup_rx(cfg, token)
        if rx is None:
            return False, "unknown command", []
        rxs.append(rx)

    failures: List[Tuple[str, str, str]] = []
    if tx is not None:
        failures = await _apply_amx_command_to_rx_aliases(
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
    return True, cmd, failures


async def _refresh_hdmi_outputs(*, cfg: Config, amx: Any, state: NhdState, timeout_ms: int) -> None:
    if not hasattr(amx, "get_hdmi_output"):
        return
    rx_aliases = sorted(cfg.rx_by_alias.keys(), key=_rx_alias_sort_key)
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


async def _refresh_hdmi_outputs_for_aliases(
    *,
    cfg: Config,
    amx: Any,
    state: NhdState,
    timeout_ms: int,
    rx_aliases: List[str],
) -> None:
    if not hasattr(amx, "get_hdmi_output"):
        return
    wanted = [a for a in rx_aliases if a in cfg.rx_by_alias]
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


async def _apply_amx_command_to_rx_aliases(
    *,
    cfg: Config,
    amx: Any,
    state: NhdState,
    rx_aliases: List[str],
    command: str,
    timeout_ms: int,
) -> List[Tuple[str, str, str]]:
    if not rx_aliases:
        return []
    failures: List[Tuple[str, str, str]] = []
    if hasattr(amx, "send_command_and_get_hdmi_output"):
        results = await asyncio.gather(
            *(
                amx.send_command_and_get_hdmi_output(
                    decoder_ip=cfg.rx_by_alias[a].amx_decoder_ip,
                    command=command,
                    timeout_ms=timeout_ms,
                )
                for a in rx_aliases
            ),
            return_exceptions=True,
        )
        for a, res in zip(rx_aliases, results):
            if isinstance(res, BaseException):
                failures.append((a, cfg.rx_by_alias[a].amx_decoder_ip, str(res)))
                state.set_rx_online(a, False)
                state.set_rx_hdmi_output(a, None)
            else:
                state.set_rx_online(a, True)
                state.set_rx_hdmi_output(a, res if isinstance(res, bool) else None)
        return failures

    if hasattr(amx, "send_command"):
        results = await asyncio.gather(
            *(
                amx.send_command(
                    decoder_ip=cfg.rx_by_alias[a].amx_decoder_ip,
                    command=command,
                )
                for a in rx_aliases
            ),
            return_exceptions=True,
        )
        for a, res in zip(rx_aliases, results):
            if isinstance(res, BaseException):
                failures.append((a, cfg.rx_by_alias[a].amx_decoder_ip, str(res)))
                state.set_rx_online(a, False)
                state.set_rx_hdmi_output(a, None)
            else:
                state.set_rx_online(a, True)
        if rx_aliases:
            asyncio.create_task(
                _refresh_hdmi_outputs_for_aliases(
                    cfg=cfg,
                    amx=amx,
                    state=state,
                    timeout_ms=max(200, min(1200, int(timeout_ms))),
                    rx_aliases=rx_aliases,
                )
            )
    return failures


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
    runtime: RuntimeSettings,
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
            parts = line.split()
            parts_lower = [p.lower() for p in parts]

            # Handle known commands.
            if len(parts) >= 4 and parts_lower[:2] == ["matrix", "set"]:
                ok = False
                resp = "unknown command"
                failures: List[Tuple[str, str, str]] = []
                try:
                    ok, resp, failures = await _handle_matrix_set(cfg, amx, state, line, runtime.amx_verify_timeout_ms)
                    if ok:
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

                        # Optional AMX verification (problems-only)
                        if (not cfg.amx_dry_run) and runtime.amx_verify_after_set and tx_alias is not None:
                            tx_obj2 = _lookup_tx(cfg, tx_alias)
                            expected = tx_obj2.amx_stream if tx_obj2 is not None else None
                            if expected:
                                for rx_a in rx_aliases:
                                    ip = cfg.rx_by_alias[rx_a].amx_decoder_ip
                                    ok_v = await amx.verify_stream(
                                        decoder_ip=ip,
                                        expected_stream=expected,
                                        timeout_ms=runtime.amx_verify_timeout_ms,
                                    )
                                    if not ok_v:
                                        await notifier.problem(
                                            f"amx.verify.{ip}",
                                            f"DT: ERROR AMX verify failed: {rx_a} expected STREAM {expected}",
                                        )
                except Exception as e:
                    LOG.exception("AMX routing failed")
                    await notifier.problem("amx.route", f"DT: ERROR AMX route failed: {e}")
                    # Keep RTI driver happy: still mirror the command as success.
                    ok, resp = True, line

                if failures:
                    for (rx_a, ip, err) in failures:
                        state.set_rx_online(rx_a, False)
                        await notifier.problem(f"amx.set.{ip}", f"DT: ERROR AMX route failed: {rx_a} ({ip}): {err}")
                    await notifier.problem(
                        "amx.route.partial",
                        "DT: ERROR AMX route failed on: "
                        + ", ".join(f"{rx_a}({ip})" for (rx_a, ip, _e) in failures[:3])
                        + (" ..." if len(failures) > 3 else ""),
                    )
                else:
                    # Mark RX as online on successful send
                    try:
                        for tok in parts[3:]:
                            rx_obj = _lookup_rx(cfg, tok)
                            if rx_obj is not None:
                                state.set_rx_online(rx_obj.alias, True)
                    except Exception:
                        pass

                writer.write(_crlf(resp if ok else "unknown command"))
                await writer.drain()
                continue

            # Breakaway switching
            if len(parts) >= 5 and parts_lower[0] == "matrix" and parts_lower[2] == "set":
                # matrix <kind> set <TX|NULL> <RX...>
                if len(parts) >= 5 and parts_lower[0] == "matrix" and parts_lower[2] == "set":
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
                                failures = await _apply_amx_command_to_rx_aliases(
                                    cfg=cfg,
                                    amx=amx,
                                    state=state,
                                    rx_aliases=rx_aliases,
                                    command=f"set:{tx_obj.amx_stream}",
                                    timeout_ms=runtime.amx_verify_timeout_ms,
                                )
                                if failures:
                                    for (rx_a, ip, err) in failures:
                                        await notifier.problem(
                                            f"amx.set.{ip}",
                                            f"DT: ERROR AMX breakaway video route failed: {rx_a} ({ip}): {err}",
                                        )
                                    await notifier.problem(
                                        "amx.breakaway.video.partial",
                                        "DT: ERROR AMX breakaway video route failed on: "
                                        + ", ".join(f"{rx_a}({ip})" for (rx_a, ip, _e) in failures[:3])
                                        + (" ..." if len(failures) > 3 else ""),
                                    )

                                if (not cfg.amx_dry_run) and runtime.amx_verify_after_set:
                                    expected = tx_obj.amx_stream
                                    for rx_a in rx_aliases:
                                        ip = cfg.rx_by_alias[rx_a].amx_decoder_ip
                                        ok_v = await amx.verify_stream(
                                            decoder_ip=ip,
                                            expected_stream=expected,
                                            timeout_ms=runtime.amx_verify_timeout_ms,
                                        )
                                        if not ok_v:
                                            await notifier.problem(
                                                f"amx.verify.{ip}",
                                                f"DT: ERROR AMX verify failed: {rx_a} expected STREAM {expected}",
                                            )
                            except Exception as e:
                                LOG.exception("AMX routing failed")
                                await notifier.problem(
                                    "amx.breakaway.video", f"DT: ERROR AMX breakaway video route failed: {e}"
                                )

                        writer.write(_crlf(line))  # command mirror ack
                        await writer.drain()
                        continue

            # Matrix query commands used for RTI feedback variables
            if len(parts) >= 2 and parts_lower[0] == "matrix" and "get" in parts_lower:
                # matrix get [<RX...>]  — primary all-media assignments (§13.3)
                if len(parts) >= 2 and parts_lower[0] == "matrix" and parts_lower[1] == "get":
                    rx_tokens = parts[2:]
                    rx_aliases_m: List[str] = []
                    if rx_tokens:
                        for tok in rx_tokens:
                            rx_obj = _lookup_rx(cfg, tok)
                            if rx_obj is None:
                                writer.write(_crlf("unknown command"))
                                await writer.drain()
                                break
                            rx_aliases_m.append(rx_obj.alias)
                        else:
                            pass
                    else:
                        rx_aliases_m = list(cfg.rx_by_alias.keys())
                    if len(rx_tokens) and len(rx_aliases_m) != len(rx_tokens):
                        continue
                    for resp_line in _format_matrix_info(
                        heading="matrix", mapping=state.video, rx_aliases=rx_aliases_m
                    ):
                        writer.write(_crlf(resp_line))
                    await writer.drain()
                    continue
                # Examples:
                # matrix video get [<RX...>]
                # matrix audio get [<RX...>]
                if len(parts) >= 3 and parts_lower[0] == "matrix" and parts_lower[2] == "get":
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
                    _unknown_ctl_record(line)
                    writer.write(_crlf("unknown command"))
                await writer.drain()
                continue

            if (
                len(parts) >= 6
                and parts_lower[0] == "config"
                and parts_lower[1] == "set"
                and parts_lower[2] == "device"
                and parts_lower[3] == "cec"
            ):
                cec_mode = parts_lower[4]
                hdmi_enabled: Optional[bool] = None
                if cec_mode in ("onetouchplay", "on"):
                    hdmi_enabled = True
                elif cec_mode in ("standby", "off"):
                    hdmi_enabled = False
                else:
                    _unknown_ctl_record(line)
                    writer.write(_crlf("unknown command"))
                    await writer.drain()
                    continue

                rx_aliases: List[str] = []
                for tok in parts[5:]:
                    rx_obj = _lookup_rx(cfg, tok)
                    if rx_obj is None:
                        writer.write(_crlf("unknown command"))
                        await writer.drain()
                        break
                    rx_aliases.append(rx_obj.alias)
                else:
                    LOG.info(
                        "CEC translate: %s -> %s for %d RX(s): %s",
                        cec_mode,
                        "hdmiOn" if hdmi_enabled else "hdmiOff",
                        len(rx_aliases),
                        ", ".join(rx_aliases),
                    )
                    try:
                        failures = await _apply_amx_command_to_rx_aliases(
                            cfg=cfg,
                            amx=amx,
                            state=state,
                            rx_aliases=rx_aliases,
                            command="hdmiOn" if hdmi_enabled else "hdmiOff",
                            timeout_ms=runtime.amx_verify_timeout_ms,
                        )
                    except Exception as e:
                        LOG.exception("AMX HDMI control failed")
                        await notifier.problem("amx.hdmi", f"DT: ERROR AMX HDMI control failed: {e}")
                        failures = []

                    if failures:
                        for (rx_a, ip, err) in failures:
                            await notifier.problem(
                                f"amx.hdmi.{ip}",
                                f"DT: ERROR AMX HDMI control failed: {rx_a} ({ip}): {err}",
                            )
                        await notifier.problem(
                            "amx.hdmi.partial",
                            "DT: ERROR AMX HDMI control failed on: "
                            + ", ".join(f"{rx_a}({ip})" for (rx_a, ip, _e) in failures[:3])
                            + (" ..." if len(failures) > 3 else ""),
                        )
                    writer.write(_crlf(line))
                    await writer.drain()
                    continue

            if lower.startswith("config set "):
                # For many config set commands, WyreStorm replies with command mirror.
                writer.write(_crlf(line))
                await writer.drain()
                continue

            if lower.startswith("config get "):
                _cg_out = _handle_config_get(cfg, session, state, line)
                if _cg_out == ["unknown command"]:
                    _unknown_ctl_record(line)
                for resp_line in _cg_out:
                    writer.write(_crlf(resp_line))
                await writer.drain()
                continue

            # Safe mirrors / minimal responses for RTI driver feature surface.
            # Video wall + multiview query commands (return empty lists rather than "unknown command")
            if lower.startswith(("scene get", "vw get", "wscene2 get")):
                _vw_out = _handle_videowall_get(cfg, line)
                if _vw_out == ["unknown command"]:
                    _unknown_ctl_record(line)
                for resp_line in _vw_out:
                    writer.write(_crlf(resp_line))
                await writer.drain()
                continue

            if lower.startswith(("mscene get", "mview get")):
                _mv_out = _handle_multiview_get(cfg, line)
                if _mv_out == ["unknown command"]:
                    _unknown_ctl_record(line)
                for resp_line in _mv_out:
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

            # Unknown command (no handler matched)
            _unknown_ctl_record(line)
            writer.write(_crlf("unknown command"))
            await writer.drain()

    finally:
        LOG.info("RTI disconnected from %s", peer)
        health.rti_clients = max(0, health.rti_clients - 1)
        writer.close()
        with contextlib.suppress(Exception):
            await writer.wait_closed()


async def run_server(*, cfg: Config, config_path: str, listen: str, port: int) -> None:
    started_at = time.monotonic()
    _cfg_dir = Path(config_path).expanduser().resolve().parent
    _unknown_ctl_configure(
        enabled=cfg.unknown_ctl_enabled,
        config_dir=_cfg_dir,
        persist_path=cfg.unknown_ctl_persist_path,
    )
    _unknown_ctl_load_from_disk()
    # Optional RTI UDP control listener (e.g., reboot command)
    if cfg.rti_control_enabled and cfg.rti_control_port > 0:
        loop = asyncio.get_running_loop()
        bind = cfg.rti_control_bind_address or "0.0.0.0"
        await loop.create_datagram_endpoint(
            lambda: _RtiControlUdp(cfg=cfg),
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
    elif cfg.amx_persistent:
        LOG.warning("AMX persistent mode enabled: keeping per-decoder sockets open.")
        amx = PersistentAmxClient(
            decoder_port=cfg.amx_decoder_port,
            connect_timeout_ms=cfg.amx_connect_timeout_ms,
            command_timeout_ms=cfg.amx_command_timeout_ms,
            keepalive_seconds=cfg.amx_keepalive_seconds,
            bind_address=cfg.amx_bind_address,
            set_queue_limit=cfg.amx_set_queue_limit,
            set_retry_attempts=cfg.amx_set_retry_attempts,
            set_retry_backoff_initial_ms=cfg.amx_set_retry_backoff_initial_ms,
            set_retry_backoff_max_ms=cfg.amx_set_retry_backoff_max_ms,
        )
    else:
        amx = AmxClient(
            decoder_port=cfg.amx_decoder_port,
            connect_timeout_ms=cfg.amx_connect_timeout_ms,
            command_timeout_ms=cfg.amx_command_timeout_ms,
            bind_address=cfg.amx_bind_address,
            set_retry_attempts=cfg.amx_set_retry_attempts,
            set_retry_backoff_initial_ms=cfg.amx_set_retry_backoff_initial_ms,
            set_retry_backoff_max_ms=cfg.amx_set_retry_backoff_max_ms,
        )

    state = NhdState(cfg)
    if cfg.amx_dry_run and cfg.amx_dry_run_offline_decoders:
        offline = {x.strip() for x in cfg.amx_dry_run_offline_decoders if str(x).strip()}
        for rx in cfg.rx_by_alias.values():
            if rx.amx_decoder_ip in offline:
                state.set_rx_online(rx.alias, False)
    health = HealthState()
    runtime = RuntimeSettings(cfg)
    problems = ProblemState()

    notifier = RtiNotifier(
        enabled=cfg.rti_notify_enabled,
        protocol=cfg.rti_notify_protocol,
        host=cfg.rti_notify_host,
        port=cfg.rti_notify_port,
        bind_address=cfg.rti_notify_bind_address,
        min_interval_seconds=cfg.rti_notify_min_interval_seconds,
        repeat_suppression_seconds=cfg.rti_notify_repeat_suppression_seconds,
    )
    notifier.attach_problem_state(problems)
    await notifier.start()

    # Optional AMX self-test on startup (problems-only notification)
    if cfg.amx_self_test_on_start:
        res = await _amx_self_test(cfg=cfg, amx=amx)
        fail = res.get("unreachable") or []
        if fail:
            fail_l = list(fail)
            await notifier.problem(
                "amx.selftest",
                f"DT: ERROR AMX self-test: {res.get('ok', 0)}/{res.get('total', 0)} reachable. Unreachable: {', '.join(fail_l[:5])}"
                + (" ..." if len(fail_l) > 5 else ""),
            )

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
        runtime=runtime,
    )
    await status.start()

    if cfg.http_status_enabled:
        http_server = await asyncio.start_server(
            lambda r, w: _handle_http_client(
                r,
                w,
                cfg=cfg,
                health=health,
                amx=amx,
                state=state,
                runtime=runtime,
                problems=problems,
                started_at=started_at,
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
    _validate_config(cfg)

    try:
        asyncio.run(
            run_server(cfg=cfg, config_path=args.config, listen=args.listen, port=args.port)
        )
    except KeyboardInterrupt:
        return 130
    return 0

