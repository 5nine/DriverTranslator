#!/usr/bin/env python3
"""
Generate RTI Two Way Strings .driverconfig for DriverTranslator telemetry.

Pilot default: 10 TX (IN1-BOX1 .. IN10-BOX10), one DTRXSUMMARY string, two fault booleans per TX:
overall error (status=error) and TX offline (tx-state=disconnected). DT listens 100.64.200.21:4999; RTI connects.

Usage:
  python tools/generate_rti_twoway_driverconfig.py -o untracked/drivertranslator-twoway-pilot10.driverconfig
  python tools/generate_rti_twoway_driverconfig.py --tx-count 12 --net-port 30002
"""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import List

# Two Way Strings driver GUID (from RTI export example).
DRIVER_GUID = "{A25A015F-C758-4E06-9D5A-EBFEFB21B3CB}"

# varType: 0 = string/integer, 1 = boolean (per project notes).
VAR_STRING = "0"
VAR_BOOLEAN = "1"


def _tx_aliases(count: int) -> List[str]:
    return [f"IN{i}-BOX{i}" for i in range(1, count + 1)]


def _slot_definitions(tx_aliases: List[str]) -> List[dict]:
    """
    Quoted key=value wire format (same prefix/suffix model as RTI reference driverconfig):
    extract text between prefix and suffix ", compare to rxTrue for booleans.

    Wire: DTTX IN1-BOX1 status="ok" hdmi-state="connected" tx-state="connected"
    """
    slots: List[dict] = []
    slots.append(
        {
            "name": "RXSummary",
            "match": 'DTRXSUMMARY "',
            "var_type": VAR_STRING,
            "prefix": 'DTRXSUMMARY "',
            "suffix": '"',
            "true_value": "",
        }
    )
    for alias in tx_aliases:
        slots.append(
            {
                "name": f"{alias} error",
                "match": f'DTTX {alias} status="',
                "var_type": VAR_BOOLEAN,
                "prefix": 'status="',
                "suffix": '"',
                "true_value": "error",
            }
        )
        slots.append(
            {
                "name": f"{alias} offline",
                "match": f'DTTX {alias} tx-state="',
                "var_type": VAR_BOOLEAN,
                "prefix": 'tx-state="',
                "suffix": '"',
                "true_value": "disconnected",
            }
        )
    return slots


def _emit_boilerplate(lines: List[str]) -> None:
    lines.append('\t\t<setting variable="ConnectionType">0</setting>')
    lines.append('\t\t<setting variable="DebugTrace">false</setting>')
    lines.append('\t\t<setting variable="enableStartByte">false</setting>')
    lines.append('\t\t<setting variable="enableStopByte">true</setting>')
    lines.append('\t\t<setting variable="pingString">PING</setting>')
    lines.append('\t\t<setting variable="pingTime">0</setting>')
    lines.append('\t\t<setting variable="serialbaudrate">9600</setting>')
    lines.append('\t\t<setting variable="serialdatabits">8</setting>')
    lines.append('\t\t<setting variable="serialparity">None</setting>')
    lines.append('\t\t<setting variable="serialstopbits">1</setting>')
    lines.append('\t\t<setting variable="startChar">%ff</setting>')
    lines.append('\t\t<setting variable="stopChar">%0a</setting>')
    for n in range(1, 101):
        lines.append(f'\t\t<setting variable="rxMultiplier{n}">1</setting>')
    for n in range(1, 101):
        lines.append(f'\t\t<setting variable="txString{n}type">0</setting>')
    for n in range(1, 101):
        lines.append(f'\t\t<setting variable="varType{n}">{VAR_STRING}</setting>')


def _xml_escape(s: str) -> str:
    """Escape for XML element text. Keep literal quotes (RTI export uses status=\" not &quot;)."""
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def build_driverconfig(
    *,
    net_address: str,
    net_port: int,
    tx_aliases: List[str],
) -> str:
    slots = _slot_definitions(tx_aliases)
    lines: List[str] = [
        '<?xml version="1.0" encoding="utf-8"?>',
        '<ConfigurationSettings version="1.0">',
        f'\t<driver id="{DRIVER_GUID}">',
        f'\t\t<setting variable="NetAddress">{_xml_escape(net_address)}</setting>',
        f'\t\t<setting variable="NetPort">{net_port}</setting>',
    ]
    _emit_boilerplate(lines)

    for idx, slot in enumerate(slots, start=1):
        lines.append(f'\t\t<setting variable="rxPrefix{idx}">{_xml_escape(slot["prefix"])}</setting>')
        lines.append(f'\t\t<setting variable="rxSuffix{idx}">{_xml_escape(slot["suffix"])}</setting>')
        lines.append(f'\t\t<setting variable="rxString{idx}">{_xml_escape(slot["match"])}</setting>')
        lines.append(
            f'\t\t<setting variable="rxString{idx}Name">{_xml_escape(slot["name"])}</setting>'
        )
        lines.append(f'\t\t<setting variable="varType{idx}">{slot["var_type"]}</setting>')
        if slot["var_type"] == VAR_BOOLEAN and slot["true_value"]:
            lines.append(
                f'\t\t<setting variable="rxTrue{idx}">{_xml_escape(slot["true_value"])}</setting>'
            )

    lines.append("\t</driver>")
    lines.append("</ConfigurationSettings>")
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate RTI Two Way Strings driverconfig")
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=Path("untracked/drivertranslator-twoway-pilot10.driverconfig"),
    )
    parser.add_argument("--tx-count", type=int, default=10)
    parser.add_argument(
        "--net-address",
        default="100.64.200.21",
        help="DriverTranslator IP (RTI Two Way TCP Connection target)",
    )
    parser.add_argument("--net-port", type=int, default=4999, help="DriverTranslator Two Way listen port")
    args = parser.parse_args()

    aliases = _tx_aliases(max(1, args.tx_count))
    xml = build_driverconfig(
        net_address=args.net_address,
        net_port=args.net_port,
        tx_aliases=aliases,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(xml, encoding="utf-8")
    n_bool = len(aliases) * 2
    print(f"Wrote {args.output}")
    print(f"  RX slots used: {1 + n_bool} (1 summary + {n_bool} TX booleans)")
    print(f"  TX aliases: {aliases[0]} .. {aliases[-1]}")


if __name__ == "__main__":
    main()
