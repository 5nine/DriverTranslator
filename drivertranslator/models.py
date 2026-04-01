from __future__ import annotations

import asyncio
import collections
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set


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
    tx_skipped_aliases: Set[str]
    rx_skipped_aliases: Set[str]
    amx_decoder_port: int
    amx_connect_timeout_ms: int
    amx_command_timeout_ms: int
    expanded_log: bool
    amx_dry_run: bool
    amx_persistent: bool
    amx_keepalive_seconds: int
    amx_bind_address: Optional[str]
    amx_dry_run_offline_decoders: List[str]
    amx_verify_after_set: bool
    amx_verify_timeout_ms: int
    amx_set_queue_limit: int
    amx_self_test_on_start: bool
    amx_tx_poll_enabled: bool
    amx_rx_poll_enabled: bool
    amx_set_retry_attempts: int
    amx_set_retry_backoff_initial_ms: int
    amx_set_retry_backoff_max_ms: int
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


class NhdCtlSession:
    def __init__(self) -> None:
        self.alias_mode: bool = True  # default per doc: on


class ControllerState:
    """
    Shared state across sessions to emulate the controller.

    RTI drivers commonly rely on matrix query commands to populate feedback variables.

    Concurrency model:
    - This object is designed for single-threaded asyncio use (one event loop).
    - All mutation methods are synchronous and contain no `await`, so each mutation runs to completion
      without yielding control to other coroutines.
    - If future changes introduce multi-threaded access (threads, executors, multiple loops), add an
      explicit lock and convert mutations/snapshots to `async` methods (or funnel all updates through
      a single state-owner task).
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
        # Best-effort HDMI sink link state from AMX status (True=connected, False=disconnected, None=unknown).
        self.rx_hdmi_link: Dict[str, Optional[bool]] = {rx.alias: None for rx in cfg.rx_by_alias.values()}
        # TX status is refreshed in background every 30 seconds.
        self.tx_online: Dict[str, bool] = {tx.alias: True for tx in cfg.tx_by_alias.values()}
        self.tx_status_fields: Dict[str, Dict[str, str]] = {tx.alias: {} for tx in cfg.tx_by_alias.values()}

    def set_rx_online(self, rx_alias: str, online: bool) -> None:
        if rx_alias in self.rx_online:
            self.rx_online[rx_alias] = bool(online)

    def set_rx_hdmi_output(self, rx_alias: str, enabled: Optional[bool]) -> None:
        if rx_alias in self.rx_hdmi_output:
            self.rx_hdmi_output[rx_alias] = enabled

    def set_rx_hdmi_link(self, rx_alias: str, connected: Optional[bool]) -> None:
        if rx_alias in self.rx_hdmi_link:
            self.rx_hdmi_link[rx_alias] = connected

    def set_tx_online(self, tx_alias: str, online: bool) -> None:
        if tx_alias in self.tx_online:
            self.tx_online[tx_alias] = bool(online)

    def set_tx_status_fields(self, tx_alias: str, fields: Dict[str, str]) -> None:
        if tx_alias in self.tx_status_fields:
            self.tx_status_fields[tx_alias] = dict(fields)

    def set_all_media(self, *, tx_alias: Optional[str], rx_aliases: List[str]) -> None:
        for rx in rx_aliases:
            self.video[rx] = tx_alias
            self.audio[rx] = tx_alias
            self.usb[rx] = tx_alias
            self.serial[rx] = tx_alias
            self.infrared[rx] = tx_alias

    def set_rx_all_media(self, *, rx_alias: str, tx_alias: Optional[str]) -> None:
        if rx_alias not in self.video:
            return
        self.video[rx_alias] = tx_alias
        self.audio[rx_alias] = tx_alias
        self.usb[rx_alias] = tx_alias
        self.serial[rx_alias] = tx_alias
        self.infrared[rx_alias] = tx_alias

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
        self.amx_dry_run: bool = cfg.amx_dry_run
        # Persistent mode is intentionally disabled; keep runtime value pinned false.
        self.amx_persistent: bool = False
        self.amx_verify_after_set: bool = cfg.amx_verify_after_set
        self.amx_verify_timeout_ms: int = cfg.amx_verify_timeout_ms
        self.amx_self_test_on_start: bool = cfg.amx_self_test_on_start
        self.amx_tx_poll_enabled: bool = cfg.amx_tx_poll_enabled
        self.amx_rx_poll_enabled: bool = cfg.amx_rx_poll_enabled
        self.expanded_log: bool = cfg.expanded_log
        self.http_log_lines: int = cfg.http_status_log_lines

    async def snapshot(self) -> Dict[str, Any]:
        async with self._lock:
            return {
                "amx_dry_run": self.amx_dry_run,
                "amx_persistent": self.amx_persistent,
                "amx_verify_after_set": self.amx_verify_after_set,
                "amx_verify_timeout_ms": self.amx_verify_timeout_ms,
                "amx_self_test_on_start": self.amx_self_test_on_start,
                "amx_tx_poll_enabled": self.amx_tx_poll_enabled,
                "amx_rx_poll_enabled": self.amx_rx_poll_enabled,
                "expanded_log": self.expanded_log,
                "http_log_lines": self.http_log_lines,
            }

    async def set_bool(self, key: str, value: bool) -> None:
        async with self._lock:
            if key == "amx_dry_run":
                self.amx_dry_run = value
            elif key == "amx_persistent":
                # Persistent mode is intentionally disabled; ignore requested value.
                self.amx_persistent = False
            elif key == "amx_verify_after_set":
                self.amx_verify_after_set = value
            elif key == "amx_self_test_on_start":
                self.amx_self_test_on_start = value
            elif key == "amx_rx_poll_enabled":
                self.amx_rx_poll_enabled = value
            elif key == "amx_tx_poll_enabled":
                self.amx_tx_poll_enabled = value
            elif key == "expanded_log":
                self.expanded_log = value
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
