#!/usr/bin/env bash
set -euo pipefail

# Idempotent install for Ubuntu Server (systemd + netplan).
# Installs/updates to /opt/drivertranslator and enables systemd service.

REPO_DIR_DEFAULT="/opt/drivertranslator"
SERVICE_NAME="drivertranslator.service"
BRANCH_DEFAULT="main"
CONFIG_PATH_DEFAULT="/opt/drivertranslator/config.json"
NETWORK_CONFIG_PATH_DEFAULT="/opt/drivertranslator/network_config.json"

usage() {
  cat <<'EOF'
install_drivertranslator.sh

Usage:
  sudo bash ./linux/bin/install_drivertranslator.sh --repo-dir /opt/drivertranslator --branch main

Optional flags:
  --no-network     Skip NIC static IP configuration
  --no-config      Skip generating / updating config.json
  --config PATH    Config path (default: /opt/drivertranslator/config.json)
  --network-config PATH  Network config path (default: /opt/drivertranslator/network_config.json)

Notes:
- Run from inside a git clone of the project (or provide an existing repo-dir that is a clone).
- Safe to re-run; it will (re)install dependencies, (re)copy the systemd unit, and restart the service.
 - Networking changes can drop your SSH session. Prefer running from the local console for first-time NIC setup.
EOF
}

REPO_DIR="$REPO_DIR_DEFAULT"
BRANCH="$BRANCH_DEFAULT"
CONFIG_PATH="$CONFIG_PATH_DEFAULT"
NETWORK_CONFIG_PATH="$NETWORK_CONFIG_PATH_DEFAULT"
DO_NETWORK=1
DO_CONFIG=1

while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo-dir) REPO_DIR="$2"; shift 2;;
    --branch) BRANCH="$2"; shift 2;;
    --config) CONFIG_PATH="$2"; shift 2;;
    --network-config) NETWORK_CONFIG_PATH="$2"; shift 2;;
    --no-network) DO_NETWORK=0; shift 1;;
    --no-config) DO_CONFIG=0; shift 1;;
    -h|--help) usage; exit 0;;
    *) echo "Unknown arg: $1" >&2; usage; exit 2;;
  esac
done

if [[ "$(id -u)" -ne 0 ]]; then
  echo "Must run as root (use sudo)." >&2
  exit 1
fi

echo "[1/8] Installing OS packages"
apt-get update -y
apt-get install -y --no-install-recommends python3 python3-venv git rsync ca-certificates

echo "[2/8] Ensuring repo exists at $REPO_DIR"
mkdir -p "$REPO_DIR"

if [[ -d "$REPO_DIR/.git" ]]; then
  echo "Repo already present."
else
  if [[ -d ".git" ]]; then
    echo "Copying current working tree to $REPO_DIR"
    rsync -a --delete ./ "$REPO_DIR/"
  else
    echo "ERROR: $REPO_DIR is not a git clone and current directory is not a git repo." >&2
    echo "Either git clone the repo into $REPO_DIR or run this script from inside a clone." >&2
    exit 1
  fi
fi

echo "[3/8] Updating repo to latest ($BRANCH)"
cd "$REPO_DIR"
git fetch --all --prune
git checkout "$BRANCH"
git pull --ff-only origin "$BRANCH"

echo "[4/8] Creating venv (isolated Python runtime)"
python3 -m venv "$REPO_DIR/.venv"
"$REPO_DIR/.venv/bin/python" -m pip install --upgrade pip

# No third-party deps today, but keep this hook for later:
if [[ -f "$REPO_DIR/requirements.txt" ]]; then
  "$REPO_DIR/.venv/bin/pip" install -r "$REPO_DIR/requirements.txt"
fi

echo "[5/8] Optional: configure static IPs for both NICs"
if [[ "$DO_NETWORK" -eq 1 ]]; then
  echo "Detected NICs:"
  python3 "$REPO_DIR/linux/network/nic_setup.py" list || true
  echo ""
  echo "If this is a FIRST install and you want static IPs, answer the next questions."
  echo "If you already configured networking, you can skip by re-running with --no-network."
  echo ""

  read -r -p "Configure NIC static IPs now? (y/N): " ans
  ans="${ans:-N}"
  if [[ "$ans" =~ ^[Yy]$ ]]; then
    echo "Control NIC (RTI network):"
    read -r -p "  Interface name (e.g. eth0): " CTRL_IF
    read -r -p "  Static IPv4 address (e.g. 192.168.1.100): " CTRL_IP
    read -r -p "  Prefix (e.g. 24): " CTRL_PREFIX
    read -r -p "  Gateway (e.g. 192.168.1.1): " CTRL_GW
    read -r -p "  DNS (comma separated, blank ok): " CTRL_DNS

    echo "AVoIP NIC (AMX network):"
    read -r -p "  Interface name (e.g. eth1): " AV_IF
    read -r -p "  Static IPv4 address (e.g. 192.168.10.100): " AV_IP
    read -r -p "  Prefix (e.g. 24): " AV_PREFIX
    read -r -p "  Gateway (blank recommended if same subnet): " AV_GW

    python3 - <<PY
import json
from pathlib import Path

ctrl_dns = [x.strip() for x in "${CTRL_DNS}".split(",") if x.strip()]
av_gw = "${AV_GW}".strip() or None

cfg = {
  "mode": "netplan",
  "control": {
    "match": { "interface": "${CTRL_IF}", "mac": None },
    "ipv4": { "address": "${CTRL_IP}", "prefix": int("${CTRL_PREFIX}"), "gateway": "${CTRL_GW}", "dns": ctrl_dns }
  },
  "avoip": {
    "match": { "interface": "${AV_IF}", "mac": None },
    "ipv4": { "address": "${AV_IP}", "prefix": int("${AV_PREFIX}"), "gateway": av_gw, "dns": [] }
  }
}

Path("${NETWORK_CONFIG_PATH}").write_text(json.dumps(cfg, indent=2) + "\\n", encoding="utf-8")
print("Wrote", "${NETWORK_CONFIG_PATH}")
PY

    echo "Applying netplan (this may drop SSH)..."
    python3 "$REPO_DIR/linux/network/nic_setup.py" apply-netplan --config "$NETWORK_CONFIG_PATH"
  fi
fi

echo "[6/8] Optional: generate config.json (TX/RX size + sequential IPs)"
if [[ "$DO_CONFIG" -eq 1 ]]; then
  if [[ ! -f "$CONFIG_PATH" ]]; then
    echo "No config found at $CONFIG_PATH. Creating one now."
    read -r -p "Number of TX (Transmitters): " TX_COUNT
    read -r -p "Number of RX (Receivers): " RX_COUNT
    read -r -p "Starting TX IP (IN1), e.g. 192.168.10.11: " TX_START_IP
    read -r -p "Starting RX IP (OUT1), e.g. 192.168.10.101: " RX_START_IP

    # Which NIC/IP should AMX connections bind to? Prefer the AVoIP NIC IP if provided.
    AMX_BIND_IP=""
    if [[ -f "$NETWORK_CONFIG_PATH" ]]; then
      AMX_BIND_IP="$(python3 - <<PY
import json
from pathlib import Path
cfg=json.loads(Path("${NETWORK_CONFIG_PATH}").read_text())
print(cfg.get("avoip",{}).get("ipv4",{}).get("address",""))
PY
)"
    fi

    python3 - <<PY
import ipaddress, json
from pathlib import Path

tx_count = int("${TX_COUNT}")
rx_count = int("${RX_COUNT}")
tx_start = ipaddress.IPv4Address("${TX_START_IP}")
rx_start = ipaddress.IPv4Address("${RX_START_IP}")
amx_bind = "${AMX_BIND_IP}".strip() or None

tx = []
for i in range(tx_count):
    n = i + 1
    ip = str(tx_start + i)
    tx.append({
        "alias": f"IN{n}-BOX{n}",
        "hostname": f"NHD-120-TX-{n:012d}",
        "ip": ip,
        "amx_stream": n,
    })

rx = []
for i in range(rx_count):
    n = i + 1
    ip = str(rx_start + i)
    rx.append({
        "alias": f"OUT{n}-TV{n}",
        "hostname": f"NHD-120-RX-{(100+n):012d}",
        "ip": ip,
        "amx_decoder_ip": ip,  # adjust if AMX decoder IPs differ from RX IPs
    })

cfg = {
  "nhd_ctl": {
    "version": { "api": "1.21", "web": "8.3.1", "core": "8.3.8" },
    "ipsetting": { "ip4addr": "169.254.1.1", "netmask": "255.255.0.0", "gateway": "169.254.1.254" },
    "ipsetting2": { "ip4addr": "192.168.11.243", "netmask": "255.255.255.0", "gateway": "192.168.11.1" }
  },
  "endpoints": { "tx": tx, "rx": rx },
  "amx": {
    "decoder_port": 50002,
    "connect_timeout_ms": 1000,
    "command_timeout_ms": 1500,
    "dry_run": False,
    "persistent": True,
    "keepalive_seconds": 30,
    "bind_address": amx_bind
  },
  "server": { "send_startup_notify_endpoint_online": True },
  "rti_notify": { "enabled": False, "protocol": "udp", "host": None, "port": 0, "bind_address": None }
}

Path("${CONFIG_PATH}").write_text(json.dumps(cfg, indent=2) + "\\n", encoding="utf-8")
print("Wrote", "${CONFIG_PATH}")
PY
  else
    echo "Config already exists at $CONFIG_PATH (leaving it unchanged)."
    echo "Delete it if you want the installer to regenerate it."
  fi
fi

echo "[7/8] Installing systemd unit"
install -m 0644 "$REPO_DIR/linux/systemd/drivertranslator.service" "/etc/systemd/system/$SERVICE_NAME"
systemctl daemon-reload

echo "[8/8] Enabling + restarting service"
systemctl enable "$SERVICE_NAME"
systemctl restart "$SERVICE_NAME"
systemctl --no-pager --full status "$SERVICE_NAME" || true

echo "Done."

