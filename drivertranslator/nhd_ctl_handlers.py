from __future__ import annotations
import time
from typing import Any, Dict, List

from .config_persistence import ctl_json
from .models import Config, ControllerState, NhdCtlSession
from .protocol_helpers import (
    all_endpoint_aliases,
    device_status_rx_dict,
    device_status_tx_dict,
    lookup_rx,
    lookup_tx,
)


def handle_config_get(cfg: Config, session: NhdCtlSession, state: ControllerState, cmd: str) -> List[str]:
    parts = cmd.split()
    if parts[:3] == ["config", "get", "version"]:
        return [
            f"API version: v{cfg.nhd.api}",
            f"System version: v{cfg.nhd.web}(v{cfg.nhd.core})",
        ]

    if parts[:3] == ["config", "get", "ipsetting"]:
        ip = cfg.nhd.ipsetting
        return [f"ipsetting is:static {ip['ip4addr']} {ip['netmask']} {ip['gateway']}"]

    if parts[:3] == ["config", "get", "ipsetting2"]:
        ip = cfg.nhd.ipsetting2
        return [f"ipsetting2 is:static {ip['ip4addr']} {ip['netmask']} {ip['gateway']}"]

    if parts[:3] == ["config", "get", "newip4addr"]:
        ip = cfg.nhd.ipsetting
        return [f"ipsetting is:static {ip['ip4addr']} {ip['netmask']} {ip['gateway']}"]

    if parts[:3] == ["config", "get", "newip4addr2"]:
        ip = cfg.nhd.ipsetting2
        return [f"ipsetting2 is:static {ip['ip4addr']} {ip['netmask']} {ip['gateway']}"]

    if parts[:3] == ["config", "get", "telnet"] and len(parts) >= 4 and parts[3] == "alias":
        return ["telnet alias is on" if session.alias_mode else "telnet alias is off"]

    if parts[:3] == ["config", "get", "rs-232"] and len(parts) >= 4 and parts[3] == "alias":
        return ["rs-232 alias is on" if session.alias_mode else "rs-232 alias is off"]

    if parts[:3] == ["config", "get", "htmlLog"]:
        return ["htmlLog true"]

    if parts[:4] == ["config", "get", "userList"]:
        return ["userList null null null null null"]

    if parts[:4] == ["config", "get", "dnsserver"] and len(parts) >= 5:
        if parts[4] == "ip4addr":
            return ["dns server ip4addr is: 8.8.8.8"]
        if parts[4] == "ip4addr2":
            return ["dns server ip4addr2 is: 1.1.1.1"]

    if parts[:4] == ["config", "get", "controller", "info"]:
        info = {
            "hostname_av": cfg.nhd.ipsetting.get("ip4addr", "NHD-CTL-AV"),
            "hostname_ctl": cfg.nhd.ipsetting2.get("ip4addr", "NHD-CTL-CTL"),
            "ip_av": cfg.nhd.ipsetting.get("ip4addr", "0.0.0.0"),
            "ip_ctl": cfg.nhd.ipsetting2.get("ip4addr", "0.0.0.0"),
            "mac_av": "00:00:00:00:00:00",
            "mac_ctl": "00:00:00:00:00:01",
            "serialNumber": "00000000000000",
            "version": f"V{cfg.nhd.web}",
        }
        return ["controller info: " + ctl_json([info])]

    if parts[:4] == ["config", "get", "service", "capability"] and len(parts) >= 5:
        cap = parts[4]
        val = {
            "service_https": "false",
            "service_http": "true",
            "service_sshapi": "true",
            "service_telnettlsapi": "true",
            "service_telnetapi": "true",
        }.get(cap)
        if val is not None:
            return [f"config get service capability {cap} {val}"]

    if parts[:4] == ["config", "get", "system", "sshservice"]:
        return ["system sshservice is on"]

    if parts[:5] == ["config", "get", "system", "service", "ssh_api"] and len(parts) >= 7 and parts[6] == "port":
        return ["system service ssh_api port is 10022"]
    if parts[:5] == ["config", "get", "system", "service", "telnettls_api"] and len(parts) >= 7 and parts[6] == "port":
        return ["system service telnettls_api port is 992"]
    if parts[:5] == ["config", "get", "system", "service", "telnet_api"] and len(parts) >= 7 and parts[6] == "port":
        return ["system service telnet_api port is 23"]

    if parts[:4] == ["config", "get", "system", "realtime"]:
        return [f"RealTime:{time.strftime('%a,%d-%m-%Y,%H:%M:%S', time.gmtime())}"]
    if parts[:4] == ["config", "get", "system", "ntpserverstatus"]:
        return ["system ntpserverstatus is unreachable"]
    if parts[:4] == ["config", "get", "system", "ntpzone"]:
        return ["system ntpzone is Etc UTC"]
    if parts[:4] == ["config", "get", "system", "ntpenable"]:
        return ["system ntpenable is off"]
    if parts[:4] == ["config", "get", "system", "ntpserver"]:
        return ["system ntpserver is 0.pool.ntp.org"]
    if parts[:4] == ["config", "get", "system", "web_logout_time"]:
        return ["system web_logout_time is 1440"]
    if parts[:4] == ["config", "get", "system", "preview"] and len(parts) >= 6 and parts[5] == "fps":
        return ["system preview fps: 0"]
    if parts[:4] == ["config", "get", "system", "lcd"]:
        return ["system lcd:ipversion"]
    if parts[:4] == ["config", "get", "system", "xyte_setting"]:
        return ["xyte_setting info: " + ctl_json({"xyte_setting": {"enable": True, "register_url": "https://entry.xyte.io"}})]
    if parts[:4] == ["config", "get", "system", "xyte_status"]:
        return ["xyte status: " + ctl_json({"cloud_status": "connected", "register_status": "registered"})]
    if parts[:4] == ["config", "get", "system", "802_1x"]:
        return [
            "802_1x info: "
            + ctl_json(
                [
                    {
                        "ieee802_1x_ca_mode": "default",
                        "ieee802_1x_enable": "false",
                        "ieee802_1x_mode": "",
                        "ieee802_1x_mschapv2_password": "",
                        "ieee802_1x_mschapv2_user": "",
                        "ieee802_1x_tls_private_key_password": "",
                        "ieee802_1x_tls_user": "",
                    }
                ]
            )
        ]
    if parts[:4] == ["config", "get", "system", "ldap"]:
        return [
            "ldap info: "
            + ctl_json(
                [
                    {
                        "ldap_attr": "",
                        "ldap_base_dn": "",
                        "ldap_bind_dn": "",
                        "ldap_enable": "false",
                        "ldap_mode": "dn",
                        "ldap_password": "",
                        "ldap_uid": "",
                        "ldap_uri": "",
                    }
                ]
            )
        ]

    if parts[:3] == ["config", "get", "devicelist"]:
        # Doc: only online devices returned. We treat all configured devices as online.
        # Defensive: never emit padded aliases (RTI may cache learned names verbatim).
        names = [n.strip(" \t\r\n\u00a0") for n in all_endpoint_aliases(cfg)]
        return ["devicelist is " + " ".join(names)]

    if parts[:3] == ["config", "get", "name"]:
        # `config get name` (all), or `config get name <aliasOrHostname>`
        if len(parts) == 3:
            lines: List[str] = []
            for r in cfg.rx_by_alias.values():
                _hn = r.hostname.strip(" \t\r\n\u00a0")
                _al = r.alias.strip(" \t\r\n\u00a0")
                lines.append(f"{_hn}'s alias is {_al}")
            for t in cfg.tx_by_alias.values():
                _hn = t.hostname.strip(" \t\r\n\u00a0")
                _al = t.alias.strip(" \t\r\n\u00a0")
                lines.append(f"{_hn}'s alias is {_al}")
            return lines

        token = parts[3]
        tx = lookup_tx(cfg, token)
        if tx is not None:
            return [
                f"{tx.hostname.strip(' \t\r\n\u00a0')}'s alias is {tx.alias.strip(' \t\r\n\u00a0')}"
            ]
        rx = lookup_rx(cfg, token)
        if rx is not None:
            return [
                f"{rx.hostname.strip(' \t\r\n\u00a0')}'s alias is {rx.alias.strip(' \t\r\n\u00a0')}"
            ]
        return ["unknown command"]

    if parts[:3] == ["config", "get", "devicejsonstring"]:
        devices: List[Dict[str, Any]] = []
        for t in cfg.tx_by_alias.values():
            devices.append(
                {
                    "aliasName": t.alias,
                    "deviceType": "Transmitter",
                    "trueName": t.hostname,
                    "name": t.hostname,
                    "online": True,
                }
            )
        for r in cfg.rx_by_alias.values():
            tx_alias = state.video.get(r.alias)
            tx_obj = cfg.tx_by_alias.get(tx_alias) if tx_alias else None
            devices.append(
                {
                    "aliasName": r.alias,
                    "deviceType": "Receiver",
                    "trueName": r.hostname,
                    "name": r.hostname,
                    "txName": tx_obj.hostname if tx_obj is not None else "NULL",
                    "online": bool(state.rx_online.get(r.alias, True)),
                }
            )
        return ["device json string:" + ctl_json(devices)]

    if parts[:4] == ["config", "get", "device", "info"]:
        devices: List[Dict[str, Any]] = []
        if len(parts) == 4:
            for t in cfg.tx_by_alias.values():
                devices.append({"aliasname": t.alias, "name": t.hostname, "devicetype": "Transmitter"})
            for r in cfg.rx_by_alias.values():
                devices.append({"aliasname": r.alias, "name": r.hostname, "devicetype": "Receiver"})
        else:
            for token in parts[4:]:
                tx = lookup_tx(cfg, token)
                if tx is not None:
                    devices.append({"aliasname": tx.alias, "name": tx.hostname, "devicetype": "Transmitter"})
                    continue
                rx = lookup_rx(cfg, token)
                if rx is not None:
                    devices.append({"aliasname": rx.alias, "name": rx.hostname, "devicetype": "Receiver"})
            if not devices:
                return ["unknown command"]
        return ["devices json info: " + ctl_json({"devices": devices})]

    # Section 13.2 â€” device real-time status (100/110/140/200-tier JSON shape, API v6.6 / Appendix-style).
    if parts[:4] == ["config", "get", "device", "status"]:
        tok = parts[4:]

        def _pack(rows: List[Dict[str, str]]) -> List[str]:
            body = {"devices status": rows}
            return ["devices status info: " + ctl_json(body)]

        if len(tok) == 0:
            rows: List[Dict[str, str]] = []
            for t in cfg.tx_by_alias.values():
                rows.append(device_status_tx_dict(t))
            for r in cfg.rx_by_alias.values():
                rows.append(device_status_rx_dict(r, state))
            return _pack(rows)

        rows: List[Dict[str, str]] = []
        for token in tok:
            tx = lookup_tx(cfg, token)
            if tx is not None:
                rows.append(device_status_tx_dict(tx))
                continue
            rx = lookup_rx(cfg, token)
            if rx is not None:
                rows.append(device_status_rx_dict(rx, state))
        if rows:
            return _pack(rows)

        return ["unknown command"]

    return ["unknown command"]


def handle_multiview_get(cfg: Config, cmd: str) -> List[str]:
    parts = cmd.split()
    if parts[:2] == ["mscene", "get"]:
        # Minimal empty layout list response.
        # If RX specified, return a single "mscene list:" + that RX with no layouts.
        if len(parts) == 3:
            rx = lookup_rx(cfg, parts[2])
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
            rx = lookup_rx(cfg, parts[2])
            if rx is None:
                return ["unknown command"]
            return ["mview information:", f"{rx.alias} tile"]
        lines = ["mview information:"]
        for rx in cfg.rx_by_alias.values():
            lines.append(f"{rx.alias} tile")
        return lines

    return ["unknown command"]



def handle_videowall_get(cfg: Config, cmd: str) -> List[str]:
    parts = cmd.split()
    if parts[:2] == ["scene", "get"]:
        return ["scene list:"]
    if parts[:2] == ["vw", "get"]:
        return ["Video wall information:"]
    if parts[:2] == ["wscene2", "get"]:
        return ["wscene2 list:"]
    return ["unknown command"]