from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import Any, List, Optional

from .matrix_amx import apply_amx_command_to_rx_aliases, process_matrix_set_line
from .models import Config, ControllerState, HealthState, NhdCtlSession, RuntimeSettings
from .nhd_ctl_handlers import handle_config_get, handle_multiview_get, handle_videowall_get
from .networking import crlf_line
from .problem_reporter import LocalProblemReporter
from .rti_status import RtiStatusReporter
from .protocol_helpers import (
    as_success,
    format_matrix_info,
    lookup_rx,
    lookup_tx,
    tx_alias_from_amx_stream,
)
from .unknown_ctl import record as unknown_ctl_record

LOG = logging.getLogger("drivertranslator")

async def handle_client(
    cfg: Config,
    amx: Any,
    state: ControllerState,
    notifier: LocalProblemReporter,
    health: HealthState,
    runtime: RuntimeSettings,
    status_reporter: Optional[RtiStatusReporter],
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
) -> None:
    peer = writer.get_extra_info("peername")
    LOG.info("RTI connected from %s", peer)
    session = NhdCtlSession()
    health.rti_clients += 1

    def _write_rti_line(resp_line: str) -> None:
        wire = crlf_line(resp_line)
        if runtime.expanded_log:
            LOG.info("RTI <- %s", resp_line)
        writer.write(wire)

    async def _read_protocol_line() -> Optional[str]:
        """
        Read one controller command line, accepting CRLF, LF, or CR delimiters.
        Some RTI driver flows may emit CR-only lines during reinitialize.
        Also strip/respond to Telnet negotiation bytes (IAC sequences).
        """
        IAC = 255
        DONT = 254
        DO = 253
        WONT = 252
        WILL = 251
        SB = 250
        SE = 240

        # Telnet IAC sequences may be split across TCP reads. Keep trailing
        # partial sequences and prepend them to the next chunk.
        if not hasattr(_read_protocol_line, "_pending_telnet"):
            setattr(_read_protocol_line, "_pending_telnet", bytearray())
        pending_telnet: bytearray = getattr(_read_protocol_line, "_pending_telnet")

        async def _telnet_filter_and_respond(data: bytes) -> bytes:
            out = bytearray()
            i = 0
            while i < len(data):
                b = data[i]
                if b != IAC:
                    out.append(b)
                    i += 1
                    continue

                # IAC at end of chunk: keep for next read.
                if i + 1 >= len(data):
                    pending_telnet.extend(data[i:])
                    break
                cmd = data[i + 1]

                # Escaped IAC (0xFF 0xFF) within text stream.
                if cmd == IAC:
                    out.append(IAC)
                    i += 2
                    continue

                # Subnegotiation: IAC SB ... IAC SE
                if cmd == SB:
                    j = i + 2
                    while j + 1 < len(data):
                        if data[j] == IAC and data[j + 1] == SE:
                            j += 2
                            break
                        j += 1
                    if j >= len(data):
                        # Incomplete subnegotiation at end of chunk: keep for next read.
                        pending_telnet.extend(data[i:])
                        break
                    i = j
                    continue

                # Option negotiation: respond negatively to keep raw line protocol.
                if cmd in (DO, DONT, WILL, WONT):
                    if i + 2 < len(data):
                        opt = data[i + 2]
                        if cmd in (DO, DONT):
                            # Peer asks us to DO/DON'T -> we reply WONT.
                            writer.write(bytes([IAC, WONT, opt]))
                        else:
                            # Peer says WILL/WON'T -> we reply DONT.
                            writer.write(bytes([IAC, DONT, opt]))
                        with contextlib.suppress(Exception):
                            await writer.drain()
                        i += 3
                        continue
                    # Incomplete negotiation bytes at end of chunk.
                    pending_telnet.extend(data[i:])
                    break

                # Other 2-byte telnet command; ignore.
                i += 2

            return bytes(out)

        if not hasattr(_read_protocol_line, "_buf"):
            setattr(_read_protocol_line, "_buf", bytearray())
        buf: bytearray = getattr(_read_protocol_line, "_buf")
        while True:
            for i, b in enumerate(buf):
                if b in (10, 13):  # LF or CR
                    raw = bytes(buf[:i])
                    j = i + 1
                    # Collapse optional paired newline byte (CRLF / LFCR).
                    if j < len(buf) and buf[j] in (10, 13) and buf[j] != b:
                        j += 1
                    del buf[:j]
                    line = raw.decode("utf-8", errors="replace").strip()
                    if not line:
                        return ""
                    return line
            chunk = await reader.read(4096)
            if not chunk:
                if buf:
                    raw = bytes(buf)
                    buf.clear()
                    line = raw.decode("utf-8", errors="replace").strip()
                    return line or None
                return None
            if pending_telnet:
                chunk = bytes(pending_telnet) + chunk
                pending_telnet.clear()
            app_bytes = await _telnet_filter_and_respond(chunk)
            if app_bytes:
                buf.extend(app_bytes)

    try:
        while True:
            line = await _read_protocol_line()
            if line is None:
                break
            if not line:
                continue

            LOG.info("RTI -> %s", line)
            lower = line.lower()
            parts = line.split()
            parts_lower = [p.lower() for p in parts]
            line_norm = " ".join(parts)

            # Handle known commands.
            if len(parts) >= 4 and parts_lower[:2] == ["matrix", "set"]:
                outcome = await process_matrix_set_line(
                    cfg,
                    amx,
                    state,
                    line,
                    runtime.amx_verify_timeout_ms,
                    runtime,
                    notifier,
                    status_reporter,
                )
                _write_rti_line(outcome.rti_response)
                await writer.drain()
                continue

            # Breakaway switching
            if len(parts) >= 5 and parts_lower[0] == "matrix" and parts_lower[2] == "set":
                # matrix <kind> set <TX|NULL> <RX...>
                kind = parts[1].lower()
                tx_token = parts[3]
                rx_tokens = parts[4:]

                tx_obj = lookup_tx(cfg, tx_token)
                tx_alias = None
                if tx_obj is None:
                    if tx_token.upper() != "NULL":
                        _write_rti_line("unknown command")
                        await writer.drain()
                        continue
                else:
                    tx_alias = tx_obj.alias

                rx_aliases: List[str] = []
                for tok in rx_tokens:
                    rx_obj = lookup_rx(cfg, tok)
                    if rx_obj is None:
                        _write_rti_line("unknown command")
                        await writer.drain()
                        break
                    rx_aliases.append(rx_obj.alias)
                else:
                    state.set_breakaway(kind=kind, tx_alias=tx_alias, rx_aliases=rx_aliases)

                    # Only video breakaway affects AMX in our model
                    if kind == "video" and tx_obj is not None:
                        try:
                            failures, status_by_rx = await apply_amx_command_to_rx_aliases(
                                cfg=cfg,
                                amx=amx,
                                state=state,
                                rx_aliases=rx_aliases,
                                command=f"set:{tx_obj.amx_stream}",
                                timeout_ms=runtime.amx_verify_timeout_ms,
                            )
                            if failures:
                                for (rx_a, ip, err) in failures:
                                    LOG.error("AMX SEND FAIL breakaway decoder=%s rx=%s cmd=%r err=%s", ip, rx_a, f"set:{tx_obj.amx_stream}", err)
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

                            # Prefer AMX-reported STREAM for touched RXs; if missing/unusable, fall back to NULL.
                            failed_rx = {rx_a for (rx_a, _ip, _err) in failures}
                            for rx_a in rx_aliases:
                                if rx_a in failed_rx:
                                    state.set_breakaway(kind="video", tx_alias=None, rx_aliases=[rx_a])
                                    continue
                                stream_reported = (status_by_rx.get(rx_a, {}).get("STREAM") or "").strip()
                                if not stream_reported:
                                    state.set_breakaway(kind="video", tx_alias=tx_alias, rx_aliases=[rx_a])
                                else:
                                    amx_tx_alias = tx_alias_from_amx_stream(cfg, stream_reported)
                                    state.set_breakaway(
                                        kind="video",
                                        tx_alias=(amx_tx_alias if amx_tx_alias is not None else tx_alias),
                                        rx_aliases=[rx_a],
                                    )

                            if (not cfg.amx_dry_run) and runtime.amx_verify_after_set and (not runtime.amx_rx_poll_enabled):
                                expected = str(tx_obj.amx_stream)
                                for rx_a in rx_aliases:
                                    got = (status_by_rx.get(rx_a, {}).get("STREAM") or "").strip()
                                    ip = cfg.rx_by_alias[rx_a].amx_decoder_ip
                                    if got != expected:
                                        await notifier.problem(
                                            f"amx.verify.{ip}",
                                            f"DT: ERROR AMX verify failed: {rx_a} expected STREAM {expected}",
                                        )
                        except Exception as e:
                            LOG.exception("AMX routing failed")
                            await notifier.problem(
                                "amx.breakaway.video", f"DT: ERROR AMX breakaway video route failed: {e}"
                            )
                            # Be conservative: if AMX routing errored, don't keep an optimistic video route.
                            state.set_breakaway(kind="video", tx_alias=None, rx_aliases=rx_aliases)

                    # For matrix <kind> set, mirror the raw incoming command exactly.
                    # old (normalized mirror): _write_rti_line(line_norm)
                    _write_rti_line(line)  # command mirror ack
                    await writer.drain()
                    continue

            # Matrix query commands used for RTI feedback variables
            if len(parts) >= 2 and parts_lower[0] == "matrix" and "get" in parts_lower:
                # matrix get [<RX...>]  â€” primary all-media assignments (Â§13.3)
                if len(parts) >= 2 and parts_lower[0] == "matrix" and parts_lower[1] == "get":
                    rx_tokens = parts[2:]
                    rx_aliases_m: List[str] = []
                    if rx_tokens:
                        for tok in rx_tokens:
                            rx_obj = lookup_rx(cfg, tok)
                            if rx_obj is None:
                                _write_rti_line("unknown command")
                                await writer.drain()
                                break
                            rx_aliases_m.append(rx_obj.alias)
                        else:
                            pass
                    else:
                        rx_aliases_m = list(cfg.rx_by_alias.keys())
                    if len(rx_tokens) and len(rx_aliases_m) != len(rx_tokens):
                        continue
                    matrix_lines = format_matrix_info(
                        heading="matrix", mapping=state.video, rx_aliases=rx_aliases_m
                    )
                    for resp_line in matrix_lines:
                        _write_rti_line(resp_line)
                        await writer.drain()
                    # Match observed WyreStorm framing: terminate matrix blocks with blank lines.
                    _write_rti_line("")
                    await writer.drain()
                    _write_rti_line("")
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
                            rx_obj = lookup_rx(cfg, tok)
                            if rx_obj is None:
                                _write_rti_line("unknown command")
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
                        _write_rti_line("unknown command")
                        await writer.drain()
                        continue

                    matrix_lines = format_matrix_info(
                        heading=f"matrix {kind}", mapping=table, rx_aliases=rx_aliases
                    )
                    for resp_line in matrix_lines:
                        _write_rti_line(resp_line)
                        await writer.drain()
                    # Match observed WyreStorm framing: terminate matrix blocks with blank lines.
                    _write_rti_line("")
                    await writer.drain()
                    _write_rti_line("")
                    await writer.drain()
                    continue

            if lower.startswith("config set session alias "):
                parts = line.split()
                if len(parts) == 5 and parts[4].lower() in ("on", "off"):
                    session.alias_mode = parts[4].lower() == "on"
                    _write_rti_line(line_norm)  # command mirror ack
                else:
                    unknown_ctl_record(line)
                    _write_rti_line("unknown command")
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
                    unknown_ctl_record(line)
                    _write_rti_line("unknown command")
                    await writer.drain()
                    continue

                rx_aliases: List[str] = []
                for tok in parts[5:]:
                    rx_obj = lookup_rx(cfg, tok)
                    if rx_obj is None:
                        _write_rti_line("unknown command")
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
                        failures, _status_by_rx = await apply_amx_command_to_rx_aliases(
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
                            LOG.error("AMX SEND FAIL cec decoder=%s rx=%s cmd=%r err=%s", ip, rx_a, "hdmiOn" if hdmi_enabled else "hdmiOff", err)
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
                    _write_rti_line(line_norm)
                    await writer.drain()
                    continue

            if lower.startswith("config set "):
                # For many config set commands, WyreStorm replies with command mirror.
                _write_rti_line(line_norm)
                await writer.drain()
                continue

            if lower.startswith("config get "):
                _cg_out = handle_config_get(cfg, session, state, line)
                if _cg_out == ["unknown command"]:
                    unknown_ctl_record(line)
                for resp_line in _cg_out:
                    _write_rti_line(resp_line)
                    await writer.drain()
                # Match observed WyreStorm framing for multi-line config get responses.
                if len(_cg_out) > 1 and _cg_out != ["unknown command"]:
                    _write_rti_line("")
                    await writer.drain()
                    _write_rti_line("")
                await writer.drain()
                continue

            # Safe mirrors / minimal responses for RTI driver feature surface.
            # Video wall + multiview query commands (return empty lists rather than "unknown command")
            if lower.startswith(("scene get", "vw get", "wscene2 get")):
                _vw_out = handle_videowall_get(cfg, line)
                if _vw_out == ["unknown command"]:
                    unknown_ctl_record(line)
                for resp_line in _vw_out:
                    _write_rti_line(resp_line)
                await writer.drain()
                continue

            if lower.startswith(("mscene get", "mview get")):
                _mv_out = handle_multiview_get(cfg, line)
                if _mv_out == ["unknown command"]:
                    unknown_ctl_record(line)
                for resp_line in _mv_out:
                    _write_rti_line(resp_line)
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
                    _write_rti_line(as_success(line_norm))
                else:
                    _write_rti_line(line_norm)
                await writer.drain()
                continue

            # Unknown command (no handler matched)
            unknown_ctl_record(line)
            _write_rti_line("unknown command")
            await writer.drain()

    finally:
        LOG.info("RTI disconnected from %s", peer)
        health.rti_clients = max(0, health.rti_clients - 1)
        writer.close()
        with contextlib.suppress(Exception):
            await writer.wait_closed()