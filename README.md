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

### Using a specific Git branch (e.g. `refactor/split-main-modules`)

**Fresh clone on that branch:**

```bash
sudo git clone -b refactor/split-main-modules https://github.com/5nine/DriverTranslator.git /opt/drivertranslator
```

**Already cloned** (switch branches):

```bash
cd /opt/drivertranslator
sudo git fetch origin
sudo git checkout refactor/split-main-modules
sudo git pull origin refactor/split-main-modules
```

Pure-Python refactors usually do **not** require reinstalling the venv. Restart the service so the running process loads the new code:

```bash
sudo systemctl restart drivertranslator
```

Smoke-test manually (adjust paths if your install differs):

```bash
/opt/drivertranslator/.venv/bin/python -m drivertranslator --config /opt/drivertranslator/config.json --listen 0.0.0.0 --port 2323 --log-level INFO
```

Press `Ctrl+C` to exit. If the manual run works but the service fails, check `journalctl -u drivertranslator -e`.

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
    "control_token": null,
    "password": "1234"
  }
}
```

Set `bind` to your **control NIC** IP so it’s only reachable on the control network.

- The status webpage requires **Basic auth**. Set `http_status.password` (installer default: `1234`).
- The status page does a **partial refresh every 5 seconds** while the browser tab is visible (matrix, logs, unknown-command list, and overview counters update without a full page reload).
- The matrix now includes **both TX and RX rows**:
  - TX rows are polled from AMX `getStatus` every **30 seconds** (and once at startup).
  - RX rows show routed source and latest known HDMI output state from command-driven AMX status updates.
- TX and RX **Skip/Unskip** controls are both in the matrix and persist to config.
- Skipped TX/RX rows stay in their **natural sorted positions** (not moved to the bottom).
- Web controls (including **reboot**) are available under the page’s **Controls** section after you log in.
- If `http_status.control_token` is set, control endpoints also require `token=...` in the URL (optional extra safety).
- **Unrecognized RTI commands** (status page) are persisted to `unknown_ctl.json` next to your config file, or to `unknown_ctl.persist_path` if set. Set `unknown_ctl.enabled` to `false` for in-memory only. **Clear list** on the page wipes the list (and the file when persistence is on).

---

## RTI setup

### WyreStorm NetworkHD (NHD-CTL) driver

Point the RTI WyreStorm NetworkHD “controller” connection to the DriverTranslator host:

- **IP**: DriverTranslator host (control NIC)
- **Port**: `2323` (TCP)

### Two Way Strings v2.7

`rti_notify` and `rti_status` telemetry were removed. Use the built-in HTTP status page for system health, logs, and controls.

- Status page: `http://<control-nic-ip>:8080/`
- JSON status: `http://<control-nic-ip>:8080/status.json`
- JSON logs: `http://<control-nic-ip>:8080/logs.json`

#### Optional UDP control (reboot) (TX from RTI)

Enable UDP reboot control (no token; exact string match). Recommended only on a private network.

Example `config.json`:

```json
{
  "rti_control": {
    "enabled": true,
    "bind_address": "0.0.0.0",
    "port": 30003,
    "reboot_command": "DT REBOOT"
  }
}
```

In RTI, create a Two Way Strings driver instance configured as **Network (UDP)** that sends to:

- **Remote IP**: DriverTranslator host (control NIC)
- **Remote Port**: `rti_control.port` (example: `30003`)

Then send this exact UDP payload:

- `DT REBOOT`

---

## `config.json` (this repo)

The repo root **`config.json`** is the **committed live-test template** for the current branch. It includes a **`_drivertranslator`** object (ignored by the loader) so you can confirm you are using the right file—check **`profile`**, **`branch`**, and **`updated`**.

On the appliance (install path is usually `/opt/drivertranslator`):

```bash
cd /opt/drivertranslator
sudo git pull origin refactor/split-main-modules
head -n 12 config.json
sudo systemctl restart drivertranslator
```

You should see `"branch": "refactor/split-main-modules"` in the JSON. Edit **`amx.bind_address`**, **`http_status.bind`**, **`endpoints`**, and **`http_status.password`** for your site.

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

In emulator mode you can force specific decoders to “act offline” so you can test local error logging and status behavior:

- **Config**: `amx.dry_run_offline_decoders` (list of decoder IPs)
- Example (mark RX14 offline in the default emulator IP scheme): `["192.168.10.114"]`

---

## Reference (protocol docs)

- `NetworkHD_API_v6.6.pdf` (WyreStorm NetworkHD / NHD-CTL)
- `NMX-ENC-N2312_NMX-DEC-N2322.DirectControlAPI.pdf` (AMX N2312/N2322 direct control)

