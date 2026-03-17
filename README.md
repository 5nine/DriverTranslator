# DriverTranslator (RTI → “WyreStorm NHD-CTL” → AMX SVSI/AVoIP)

This project runs a small Linux service that:

- **Accepts a TCP connection from an RTI processor** (the RTI “WyreStorm NetworkHD” driver expects to talk to an `NHD-CTL` controller via Telnet).
- **Emulates enough of the WyreStorm `NHD-CTL` API** to keep the RTI driver happy (acknowledgements, basic queries, optional notifications).
- **Translates matrix switching commands** (e.g. `matrix set source1 display1`) into **AMX SVSI N-Series direct socket commands** (e.g. connect to the target decoder on port `50002` and send `set:<stream>\r`).

The two PDFs in this folder are the source protocols:

- `NetworkHD_API_v6.6.pdf` (WyreStorm NetworkHD / NHD-CTL)
- `NMX-ENC-N2312_NMX-DEC-N2322.DirectControlAPI.pdf` (AMX N2312/N2322 direct control)

## What you get

- A Telnet-like server (defaults to port `2323`) that speaks the **WyreStorm NHD-CTL command grammar** for the subset RTI drivers typically use:
  - `matrix set <TX> <RX...>`
  - `config set session alias on|off`
  - `config get version`, `config get ipsetting`, `config get ipsetting2`
  - `config get devicelist`
  - `config get name [<aliasOrHostname>]`
  - plus “mirror” acknowledgements for many `config set ...` commands
- An AMX client that connects to each decoder IP on `50002` and sends the corresponding `set:<stream>\r`
- A JSON config file for your endpoint mappings
- Logging so you can see **exactly what the RTI driver sends** and fill any missing commands quickly.

## Quick start (Linux)

### Recommended OS: Ubuntu Server (no desktop)

Tested/targeted distro is **Ubuntu Server 24.04 LTS** (no desktop environment required). It’s a good fit because it ships with `systemd` and `netplan` by default.

### Install (idempotent)

On the Linux box:

1. Install git and clone your repo:

```bash
sudo apt update
sudo apt install -y git
sudo git clone https://github.com/5nine/DriverTranslator.git /opt/drivertranslator
cd /opt/drivertranslator
```

2. Copy the config:

```bash
cp config.example.json config.json
```

3. Edit `config.json` to match your system (TX aliases → AMX stream ids, RX aliases → AMX decoder IPs).

4. Run the installer (safe to re-run any time):

```bash
sudo bash ./linux/bin/install_drivertranslator.sh
```

5. In RTI, point the WyreStorm driver’s controller IP/port to this Linux machine (`2323` by default).

During install, you can choose **Offline emulator mode**. When enabled, DriverTranslator will **not open TCP connections to AMX** and will only log what it *would* have sent (useful on a bench without AMX hardware).

### Update later (pull + restart in one go)

Yes—an “executable” can absolutely do a `git pull` and update the running service.

From the Linux box:

```bash
cd /opt/drivertranslator
sudo bash ./linux/bin/update_drivertranslator.sh
```

### Monitor

```bash
cd /opt/drivertranslator
bash ./linux/bin/monitor_drivertranslator.sh status
bash ./linux/bin/monitor_drivertranslator.sh logs
```

## RTI error/status messages (Two Way Strings driver)

If you want DriverTranslator to **push** error/status messages back into RTI, configure an RTI “Two Way Strings” driver as a
listener and enable `rti_notify` in `config.json`.

- **Recommended**: use **UDP** (simple, no connection management).
- Configure the “Two Way Strings” driver as **Network (UDP)** with:
  - **Local Port** = the `rti_notify.port` you choose (example below: `30001`)
  - Add RX strings like `DT: ERROR$$*$$` or `DT: RTI connected$$*$$` to trigger events/variables.

Example config:

```json
{
  "rti_notify": {
    "enabled": true,
    "protocol": "udp",
    "host": "192.168.1.50",
    "port": 30001
  }
}
```

`host` is the IP of the RTI processor (or whatever is running the “Two Way Strings” socket).

Notes:
- DriverTranslator only sends `rti_notify` messages for **problems/errors** (no spammy “connected” chatter).
- Two anti-spam settings are available:
  - `min_interval_seconds`: minimum time between messages (per error type)
  - `repeat_suppression_seconds`: suppress identical repeats for longer

## RTI status channel (optional)

If you want periodic “heartbeat” style status (separate from problems-only `rti_notify`), enable `rti_status`.

Example:

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

## Start automatically on boot (systemd)

The installer (`./linux/bin/install_drivertranslator.sh`) installs and enables the systemd service for you.

## Emulator / bench mode (no AMX hardware required)

Use `config.emulator.json`, which:

- Defines **10 TX** and **40 RX** using the RTI driver’s recommended alias conventions:
  - TX aliases: `IN<number>-<name>` (e.g. `IN1-BluRayPlayer`)
  - RX aliases: `OUT<number>-<name>` (e.g. `OUT1-LobbyTV`)
- Uses **NHD-120 series** hostnames (e.g. `NHD-120-TX-...`, `NHD-120-RX-...`) to match the RTI driver expectations.
- Uses made-up IPs starting at **`192.168.10.11`** for TX and **`192.168.10.101`** for RX
- Enables `"amx": { "dry_run": true }` so **no TCP connections** are made; the service only logs what it would have sent to AMX.

Run:

```bash
python3 -m drivertranslator --config ./config.emulator.json --listen 0.0.0.0 --port 2323 --log-level INFO
```

### Using real Telnet port 23

On Linux, binding to port `23` typically requires root. Safer options:

- Run on `2323` (recommended), or
- Use a firewall/NAT redirect (e.g. `iptables` / `nftables`) from `23` → `2323`, or
- Grant capability to Python (`setcap cap_net_bind_service=+ep ...`) if you prefer.

## How the translation works

- RTI sends WyreStorm-style routing: `matrix set <TX> <RX1> <RX2> ...`
- The service looks up:
  - `<TX>` → AMX `stream` number (integer)
  - each `<RXn>` → AMX decoder IP
- For each RX, it sends `set:<stream>\r` to `<decoder_ip>:50002`:
  - default mode: connect → send → close
  - optional fast mode: keep one persistent socket per decoder (recommended when nothing else connects and you want fastest switching)
- The service then replies to RTI with the **WyreStorm “command mirror” acknowledgement** and `\r\n` terminator.

## Notes / limitations

- This is an **emulation shim**, not a full NHD-CTL implementation. If your RTI driver uses additional commands, the service logs them and you can add handlers.
- AMX port `50002` only allows **one connection at a time per device**. The service serializes commands per decoder to avoid collisions.

## Fast switching (persistent AMX sockets)

If you want the fastest switching and nothing else will connect to the decoders, set:

```json
{
  "amx": {
    "persistent": true,
    "keepalive_seconds": 30
  }
}
```

This keeps one TCP connection open per decoder and reuses it for each switch, with a simple watchdog keepalive (`?\r`) and auto-reconnect.

## Two NICs: control network and AVoIP network

On the live Linux box you can use **two physical NICs**: one for RTI (control) and one for AMX (AVoIP). That way RTI traffic and AMX traffic stay on separate interfaces.

1. **Control NIC** – RTI connects here. Bind the server to this NIC’s IP with `--listen`:
   ```bash
   python3 -m drivertranslator --config ./config.json --listen 192.168.1.100 --port 2323
   ```
   Use the actual IP of the control NIC (e.g. `192.168.1.100`). In RTI, set the WyreStorm driver IP to this address.

2. **AVoIP NIC** – Outbound connections to AMX decoders use this NIC. Set the IPv4 address of this interface in config:
   ```json
   {
     "amx": {
       "bind_address": "10.0.0.100"
     }
   }
   ```
   Use the actual IP of the AVoIP NIC. All connections to AMX decoders will originate from this address.

- If `bind_address` is omitted or `null`, outbound AMX connections use the system default route (single-NIC or shared NIC).
- Ensure the AVoIP NIC has a route to the decoder IPs (same subnet or correct gateway).

## Optional: configure both NICs from a file (static IPs)

If you want an installer-friendly way to set **static IPs on both NICs** and choose which physical NIC is “control” vs “AVoIP”, use:

- `linux/network/network_config.example.json` (template)
- `linux/network/nic_setup.py` (helper)

### List NICs (choose by interface name or MAC)

```bash
python3 /opt/drivertranslator/linux/network/nic_setup.py list
```

This prints lines like:

`eth0  00:11:22:33:44:55  192.168.1.10/24`

### Apply (Ubuntu / Netplan)

1. Copy the template and edit it:

```bash
cp /opt/drivertranslator/linux/network/network_config.example.json /opt/drivertranslator/network_config.json
sudo nano /opt/drivertranslator/network_config.json
```

2. Apply it (writes `/etc/netplan/99-drivertranslator.yaml` and runs `netplan apply`):

```bash
sudo python3 /opt/drivertranslator/linux/network/nic_setup.py apply-netplan --config /opt/drivertranslator/network_config.json
```

Notes:
- This assumes **control NIC** has the default gateway, and **AVoIP NIC** is same-subnet to AMX (often no gateway).
- If you need advanced policy routing (two gateways), we can extend the tool to add it.

