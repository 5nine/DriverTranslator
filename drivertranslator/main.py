import argparse
import asyncio
import contextlib
import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


LOG = logging.getLogger("drivertranslator")


def _crlf(line: str) -> bytes:
    return (line + "\r\n").encode("utf-8", errors="replace")


def _as_int(v: Any, *, default: int) -> int:
    try:
        return int(v)
    except Exception:
        return default


@dataclass(frozen=True)
class Tx:
    alias: str
    hostname: str
    amx_stream: int


@dataclass(frozen=True)
class Rx:
    alias: str
    hostname: str
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
        amx_stream = _as_int(t.get("amx_stream"), default=0)
        txs.append(Tx(alias=alias, hostname=hostname, amx_stream=amx_stream))

    rxs: List[Rx] = []
    for r in rx_list:
        alias = str(r["alias"])
        hostname = str(r.get("hostname") or f"NHD-RX-{alias}")
        ip = str(r["amx_decoder_ip"])
        rxs.append(Rx(alias=alias, hostname=hostname, amx_decoder_ip=ip))

    amx = raw.get("amx", {})
    server = raw.get("server", {})

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
    )


class AmxClient:
    def __init__(self, *, decoder_port: int, connect_timeout_ms: int, command_timeout_ms: int):
        self._decoder_port = decoder_port
        self._connect_timeout = connect_timeout_ms / 1000
        self._command_timeout = command_timeout_ms / 1000
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
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(decoder_ip, self._decoder_port),
                timeout=self._connect_timeout,
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


class NhdCtlSession:
    def __init__(self) -> None:
        self.alias_mode: bool = True  # default per doc: on


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


async def _handle_matrix_set(cfg: Config, amx: AmxClient, cmd: str) -> Tuple[bool, str]:
    # cmd: "matrix set <TX> <RX1> <RX2> ... <RXn>"
    parts = cmd.split()
    if len(parts) < 4:
        return False, "unknown command"

    tx_token = parts[2]
    rx_tokens = parts[3:]

    tx = _lookup_tx(cfg, tx_token)
    if tx is None:
        return False, "unknown command"

    rxs: List[Rx] = []
    for token in rx_tokens:
        rx = _lookup_rx(cfg, token)
        if rx is None:
            return False, "unknown command"
        rxs.append(rx)

    # Fire AMX commands (one per decoder). Do it concurrently, but each decoder is locked in AmxClient.
    await asyncio.gather(
        *(amx.set_stream(decoder_ip=rx.amx_decoder_ip, stream=tx.amx_stream) for rx in rxs),
        return_exceptions=False,
    )

    # WyreStorm ack is a "command mirror"
    return True, cmd


async def handle_client(cfg: Config, amx: AmxClient, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    peer = writer.get_extra_info("peername")
    LOG.info("RTI connected from %s", peer)
    session = NhdCtlSession()

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

            # Handle known commands.
            if line.startswith("matrix set "):
                try:
                    ok, resp = await _handle_matrix_set(cfg, amx, line)
                except Exception as e:
                    LOG.exception("AMX routing failed")
                    ok, resp = False, f"error {e}"
                writer.write(_crlf(resp if ok else "unknown command"))
                await writer.drain()
                continue

            if line.startswith("config set session alias "):
                parts = line.split()
                if len(parts) == 5 and parts[4] in ("on", "off"):
                    session.alias_mode = parts[4] == "on"
                    writer.write(_crlf(line))  # command mirror ack
                else:
                    writer.write(_crlf("unknown command"))
                await writer.drain()
                continue

            if line.startswith("config set "):
                # For many config set commands, WyreStorm replies with command mirror.
                writer.write(_crlf(line))
                await writer.drain()
                continue

            if line.startswith("config get "):
                for resp_line in _handle_config_get(cfg, session, line):
                    writer.write(_crlf(resp_line))
                await writer.drain()
                continue

            # Unknown command
            writer.write(_crlf("unknown command"))
            await writer.drain()

    finally:
        LOG.info("RTI disconnected from %s", peer)
        writer.close()
        with contextlib.suppress(Exception):
            await writer.wait_closed()


async def run_server(*, cfg: Config, listen: str, port: int) -> None:
    amx = AmxClient(
        decoder_port=cfg.amx_decoder_port,
        connect_timeout_ms=cfg.amx_connect_timeout_ms,
        command_timeout_ms=cfg.amx_command_timeout_ms,
    )

    server = await asyncio.start_server(
        lambda r, w: handle_client(cfg, amx, r, w),
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

    cfg = load_config(args.config)

    try:
        asyncio.run(run_server(cfg=cfg, listen=args.listen, port=args.port))
    except KeyboardInterrupt:
        return 130
    return 0

