# DriverTranslator (RTI → WyreStorm `NHD-CTL` emulator → AMX)

DriverTranslator is a small Linux service that:

- **Accepts a TCP connection from an RTI processor** (the RTI “WyreStorm NetworkHD” driver expects to talk to an `NHD-CTL` via Telnet).
- **Emulates the WyreStorm `NHD-CTL` API surface** RTI uses (including safe mirrors + matrix queries for feedback).
- **Translates video switching** into AMX AVoIP decoder commands on TCP `50002` (`set:<stream>\r`).

This project targets **WyreStorm NetworkHD 120 series naming** (`NHD-120-TX...`, `NHD-120-RX...`) and RTI alias conventions (`INx-...`, `OUTx-...`).

---

## Install (Ubuntu Server 24.04 LTS, no desktop)

1. Install git and clone:

```bash
sudo apt update
sudo apt install -y git
sudo git clone https://github.com/5nine/DriverTranslator.git /opt/drivertranslator
cd /opt/drivertranslator
```

2. Run the installer:

```bash
sudo bash ./linux/bin/install_drivertranslator.sh
```

If you enable **tty1 auto-login + auto-start log view** in the installer and the console log view shows **permission denied** (or doesn’t show service logs), add your console user to `systemd-journal`:

```bash
sudo usermod -aG systemd-journal <your_console_user>
```

Then **reboot** (or log out/in). On boot, the machine will show live logs; press **`Ctrl+C`** to exit to a shell.

During the installer you can choose:
- **Dual-NIC static IP setup** (control + AVoIP) via netplan
- System size (**TX/RX counts**) and starting TX/RX IPs (auto-assign sequential IPs)
- **Offline emulator mode** (no AMX TCP connections; log-only)

3. In RTI, point the WyreStorm NetworkHD driver’s controller IP/port to this Linux machine (`2323` by default).

---

## Monitor / logs

- **Service status**

```bash
cd /opt/drivertranslator
bash ./linux/bin/monitor_drivertranslator.sh status
```

- **Follow logs**

```bash
cd /opt/drivertranslator
bash ./linux/bin/monitor_drivertranslator.sh logs
```

### Show logs on the local console at boot (auto-login)

The installer can optionally configure **tty1 auto-login** and automatically run a live log view on boot.

- **Exit logs to shell**: press `Ctrl+C`
- **Disable for one session** (at the shell): `export DT_CONSOLE_LOGS=0`

---

## Local status webpage (control network)

DriverTranslator includes a small built-in web server for local status:

- **URL**: `http://<control-nic-ip>:8080/`
- **JSON**: `http://<control-nic-ip>:8080/status.json`
- **Logs (JSON)**: `http://<control-nic-ip>:8080/logs.json`

Configure in `config.json`:

```json
{
  "http_status": {
    "enabled": true,
    "bind": "192.168.1.100",
    "port": 8080,
    "log_lines": 200,
    "control_token": null
  }
}
```

Set `bind` to your **control NIC** IP so it’s only reachable on the control network.

If you want to allow changing runtime settings from the page (Controls section), set a `control_token` and include it in control URLs (example):  
`/control/set?key=amx_verify_after_set&value=false&token=YOURTOKEN`

You can also reboot the host from the webpage (requires `control_token`):  
`/control/reboot?token=YOURTOKEN`

---

## RTI feedback via “Two Way Strings”

### Problems-only notifications (`rti_notify`)

`rti_notify` is for **errors/problems only** (rate limited + deduped so RTI won’t get spammed).

- Recommended: configure the RTI “Two Way Strings” driver as **Network (UDP)**.
- Set the driver **Local Port** to match `rti_notify.port`.
- Add RX strings like `DT: ERROR$$*$$` to trigger events/variables.

Example `config.json`:

```json
{
  "rti_notify": {
    "enabled": true,
    "protocol": "udp",
    "host": "192.168.1.50",
    "port": 30001,
    "min_interval_seconds": 10,
    "repeat_suppression_seconds": 300
  }
}
```

### Periodic status / heartbeat (`rti_status`, optional)

If you want a separate, periodic status line (not errors), enable `rti_status`.

Example `config.json`:

```json
{
  "rti_status": {
    "enabled": true,
    "protocol": "udp",
    "host": "192.168.1.50",
    "port": 30002,
    "interval_seconds": 30
  }
}
```

Message format:

`DTSTATUS: mode=persistent rti_clients=1 amx_connected=12/40 tx_total=10 rx_total=40`

### Optional UDP control (reboot)

You can optionally enable a small UDP listener for RTI control commands (e.g., reboot).

Example `config.json`:

```json
{
  "rti_control": {
    "enabled": true,
    "bind_address": "0.0.0.0",
    "port": 30003,
    "token": "YOURTOKEN"
  }
}
```

Send (UDP payload):

- `reboot YOURTOKEN`
- `DT reboot YOURTOKEN`

---

## Configuration notes

- **AMX switching mode**
  - `dry_run: true` = offline emulator (log-only)
  - `persistent: true` = fastest switching (keeps one socket per decoder open)
  - default = connect/send/close per switch
- **AMX retry behavior**
  - `set_retry_attempts` = total attempts for an AMX `set:<stream>` (default 3)
  - `set_retry_backoff_initial_ms` / `set_retry_backoff_max_ms` = exponential backoff window for retries (small jitter is added)
- **Audio follows video**
  - This project assumes you use **video switching** and let audio follow video.

---

## Dual NICs (control + AVoIP)

- **Control NIC**: run the server bound to the control NIC IP:

```bash
/opt/drivertranslator/.venv/bin/python -m drivertranslator --config /opt/drivertranslator/config.json --listen 192.168.1.100 --port 2323
```

- **AVoIP NIC**: bind outbound AMX TCP sockets to the AVoIP NIC IP:

```json
{ "amx": { "bind_address": "192.168.10.100" } }
```

The installer can configure static IPs for both NICs via netplan.

If you only have **one NIC**, just skip the networking step (answer `N` when prompted, or run the installer with `--no-network`). The service works fine on a single interface.

---

## Offline emulator / bench mode

If you don’t have AMX hardware available:

- Choose **Offline emulator mode** in the installer, or
- Use `config.emulator.json` and run manually:

```bash
python3 -m drivertranslator --config ./config.emulator.json --listen 0.0.0.0 --port 2323 --log-level INFO
```

### Emulator fault injection (simulate offline RX)

In emulator mode you can force specific decoders to “act offline” so you can test `rti_notify` error reporting:

- **Config**: `amx.dry_run_offline_decoders` (list of decoder IPs)
- Example (mark RX14 offline in the default emulator IP scheme): `["192.168.10.114"]`

---

## Reference (protocol docs)

- `NetworkHD_API_v6.6.pdf` (WyreStorm NetworkHD / NHD-CTL)
- `NMX-ENC-N2312_NMX-DEC-N2322.DirectControlAPI.pdf` (AMX N2312/N2322 direct control)

