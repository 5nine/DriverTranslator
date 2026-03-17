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

1. Install Python 3.11+.
2. Copy the config:

```bash
cp config.example.json config.json
```

3. Edit `config.json` to match your system (TX aliases → AMX stream ids, RX aliases → AMX decoder IPs).
4. Run:

```bash
python3 -m drivertranslator --config ./config.json --listen 0.0.0.0 --port 2323
```

5. In RTI, point the WyreStorm driver’s controller IP/port to this Linux machine (`2323` by default).

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
- For each RX, it opens a TCP connection to `<decoder_ip>:50002`, sends `set:<stream>\r`, and closes.
- The service then replies to RTI with the **WyreStorm “command mirror” acknowledgement** and `\r\n` terminator.

## Notes / limitations

- This is an **emulation shim**, not a full NHD-CTL implementation. If your RTI driver uses additional commands, the service logs them and you can add handlers.
- AMX port `50002` only allows **one connection at a time per device**. The service serializes commands per decoder to avoid collisions.

