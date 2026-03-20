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
- Optional: enable tty1 auto-login + auto-start log view on boot.
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
  echo "Re-running with sudo (you may be prompted for password)..."
  exec sudo -E bash "$0" "$@"
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

echo "[5/9] Optional: configure static IPs for NIC(s)"
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

    read -r -p "Configure a separate AVoIP NIC too? (y/N): " AV_ENABLE
    AV_ENABLE="${AV_ENABLE:-N}"
    if [[ "$AV_ENABLE" =~ ^[Yy]$ ]]; then
      echo "AVoIP NIC (AMX network):"
      read -r -p "  Interface name (e.g. eth1): " AV_IF
      read -r -p "  Static IPv4 address (e.g. 192.168.10.100): " AV_IP
      read -r -p "  Prefix (e.g. 24): " AV_PREFIX
      read -r -p "  Gateway (blank recommended if same subnet): " AV_GW
    else
      AV_IF=""
      AV_IP=""
      AV_PREFIX=""
      AV_GW=""
    fi

    python3 - <<PY
import json
from pathlib import Path

ctrl_dns = [x.strip() for x in "${CTRL_DNS}".split(",") if x.strip()]
av_gw = "${AV_GW}".strip() or None
av_enabled = "${AV_ENABLE}".strip().lower() in ("y","yes","true","1","on")

cfg = {
  "mode": "netplan",
  "control": {
    "match": { "interface": "${CTRL_IF}", "mac": None },
    "ipv4": { "address": "${CTRL_IP}", "prefix": int("${CTRL_PREFIX}"), "gateway": "${CTRL_GW}", "dns": ctrl_dns }
  },
}
if av_enabled:
  cfg["avoip"] = {
    "match": { "interface": "${AV_IF}", "mac": None },
    "ipv4": { "address": "${AV_IP}", "prefix": int("${AV_PREFIX}"), "gateway": av_gw, "dns": [] }
  }

Path("${NETWORK_CONFIG_PATH}").write_text(json.dumps(cfg, indent=2) + "\\n", encoding="utf-8")
print("Wrote", "${NETWORK_CONFIG_PATH}")
PY

    echo "Applying netplan (this may drop SSH)..."
    python3 "$REPO_DIR/linux/network/nic_setup.py" apply-netplan --config "$NETWORK_CONFIG_PATH"
  fi
fi

echo "[6/9] Optional: generate config.json (TX/RX size + sequential IPs)"
if [[ "$DO_CONFIG" -eq 1 ]]; then
  if [[ ! -f "$CONFIG_PATH" ]]; then
    echo "No config found at $CONFIG_PATH. Creating one now."
    read -r -p "Number of TX (Transmitters): " TX_COUNT
    read -r -p "Number of RX (Receivers): " RX_COUNT
    read -r -p "Starting TX IP (IN1), e.g. 192.168.10.11: " TX_START_IP
    read -r -p "Starting RX IP (OUT1), e.g. 192.168.10.101: " RX_START_IP

    read -r -p "Offline emulator mode (no AMX TCP, log only)? (y/N): " OFFLINE
    OFFLINE="${OFFLINE:-N}"

    read -r -p "Enable AMX verify-after-switch (checks STREAM via ?)? (Y/n): " AMX_VERIFY
    AMX_VERIFY="${AMX_VERIFY:-Y}"
    read -r -p "AMX verify timeout ms (default 800): " AMX_VERIFY_TO
    AMX_VERIFY_TO="${AMX_VERIFY_TO:-800}"

    read -r -p "Enable AMX self-test on start (connect to all decoders)? (Y/n): " AMX_SELFTEST
    AMX_SELFTEST="${AMX_SELFTEST:-Y}"

    read -r -p "Enable local HTTP status page? (Y/n): " HTTP_EN
    HTTP_EN="${HTTP_EN:-Y}"
    read -r -p "HTTP status bind IP (blank=control NIC IP, default 0.0.0.0): " HTTP_BIND
    read -r -p "HTTP status port (default 8080): " HTTP_PORT
    HTTP_PORT="${HTTP_PORT:-8080}"
    read -r -p "HTTP log lines on page (default 200): " HTTP_LOGLINES
    HTTP_LOGLINES="${HTTP_LOGLINES:-200}"
    read -r -p "HTTP webpage password (Basic auth) (default 1234): " HTTP_PW
    HTTP_PW="${HTTP_PW:-1234}"

    read -r -p "Enable RTI problems-only notify (Two Way Strings)? (y/N): " RTI_PROB
    RTI_PROB="${RTI_PROB:-N}"
    if [[ "$RTI_PROB" =~ ^[Yy]$ ]]; then
      read -r -p "  RTI notify host/IP: " RTI_PROB_HOST
      read -r -p "  RTI notify port (default 30001): " RTI_PROB_PORT
      RTI_PROB_PORT="${RTI_PROB_PORT:-30001}"
    else
      RTI_PROB_HOST=""
      RTI_PROB_PORT="0"
    fi

    read -r -p "Enable RTI status heartbeat (optional)? (y/N): " RTI_STAT
    RTI_STAT="${RTI_STAT:-N}"
    if [[ "$RTI_STAT" =~ ^[Yy]$ ]]; then
      read -r -p "  RTI status host/IP: " RTI_STAT_HOST
      read -r -p "  RTI status port (default 30002): " RTI_STAT_PORT
      RTI_STAT_PORT="${RTI_STAT_PORT:-30002}"
      read -r -p "  RTI status interval seconds (default 30): " RTI_STAT_INT
      RTI_STAT_INT="${RTI_STAT_INT:-30}"
    else
      RTI_STAT_HOST=""
      RTI_STAT_PORT="0"
      RTI_STAT_INT="30"
    fi

    # Which NIC/IP should AMX connections bind to? Prefer the AVoIP NIC IP if provided.
    AMX_BIND_IP=""
    CTRL_BIND_IP=""
    if [[ -f "$NETWORK_CONFIG_PATH" ]]; then
      AMX_BIND_IP="$(python3 - <<PY
import json
from pathlib import Path
cfg=json.loads(Path("${NETWORK_CONFIG_PATH}").read_text())
print(cfg.get("avoip",{}).get("ipv4",{}).get("address",""))
PY
)"
      CTRL_BIND_IP="$(python3 - <<PY
import json
from pathlib import Path
cfg=json.loads(Path("${NETWORK_CONFIG_PATH}").read_text())
print(cfg.get("control",{}).get("ipv4",{}).get("address",""))
PY
)"
    fi

    if [[ -z "${HTTP_BIND:-}" ]]; then
      HTTP_BIND="${CTRL_BIND_IP:-0.0.0.0}"
    fi

    python3 - <<PY
import ipaddress, json
from pathlib import Path

tx_count = int("${TX_COUNT}")
rx_count = int("${RX_COUNT}")
tx_start = ipaddress.IPv4Address("${TX_START_IP}")
rx_start = ipaddress.IPv4Address("${RX_START_IP}")
amx_bind = "${AMX_BIND_IP}".strip() or None
offline = "${OFFLINE}".strip().lower() in ("y","yes","true","1","on")
amx_verify = "${AMX_VERIFY}".strip().lower() not in ("n","no","false","0","off")
amx_verify_to = int("${AMX_VERIFY_TO}")
amx_selftest = "${AMX_SELFTEST}".strip().lower() not in ("n","no","false","0","off")
http_enabled = "${HTTP_EN}".strip().lower() not in ("n","no","false","0","off")
http_bind = "${HTTP_BIND}".strip() or "0.0.0.0"
http_port = int("${HTTP_PORT}")
http_log_lines = int("${HTTP_LOGLINES}")
rti_prob_enabled = "${RTI_PROB}".strip().lower() in ("y","yes","true","1","on")
rti_prob_host = "${RTI_PROB_HOST}".strip() or None
rti_prob_port = int("${RTI_PROB_PORT}")
rti_stat_enabled = "${RTI_STAT}".strip().lower() in ("y","yes","true","1","on")
rti_stat_host = "${RTI_STAT_HOST}".strip() or None
rti_stat_port = int("${RTI_STAT_PORT}")
rti_stat_int = int("${RTI_STAT_INT}")
http_pw = "${HTTP_PW}".strip() or "1234"

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
    "dry_run": offline,
    "persistent": (not offline),
    "keepalive_seconds": 30,
    "bind_address": amx_bind,
    "verify_after_set": amx_verify,
    "verify_timeout_ms": amx_verify_to,
    "set_queue_limit": 1,
    "self_test_on_start": amx_selftest,
    "set_retry_attempts": 3,
    "set_retry_backoff_initial_ms": 200,
    "set_retry_backoff_max_ms": 1200
  },
  "server": { "expanded_log": False },
  "rti_notify": { "enabled": rti_prob_enabled, "protocol": "udp", "host": rti_prob_host, "port": rti_prob_port, "bind_address": None, "min_interval_seconds": 10, "repeat_suppression_seconds": 300 },
  "rti_status": { "enabled": rti_stat_enabled, "protocol": "udp", "host": rti_stat_host, "port": rti_stat_port, "bind_address": None, "interval_seconds": rti_stat_int },
  "http_status": { "enabled": http_enabled, "bind": http_bind, "port": http_port, "log_lines": http_log_lines, "control_token": None, "password": http_pw },
  "rti_control": { "enabled": False, "bind_address": None, "port": 0, "reboot_command": "DT REBOOT" }
}

Path("${CONFIG_PATH}").write_text(json.dumps(cfg, indent=2) + "\\n", encoding="utf-8")
print("Wrote", "${CONFIG_PATH}")
PY
  else
    echo "Config already exists at $CONFIG_PATH (leaving it unchanged)."
    echo "Delete it if you want the installer to regenerate it."
  fi
fi

echo "[7/9] Installing systemd unit"
install -m 0644 "$REPO_DIR/linux/systemd/drivertranslator.service" "/etc/systemd/system/$SERVICE_NAME"
systemctl daemon-reload

echo "[8/9] Enabling + restarting service"
systemctl enable "$SERVICE_NAME"
systemctl restart "$SERVICE_NAME"
systemctl --no-pager --full status "$SERVICE_NAME" || true

echo "[9/9] Optional: auto-login on console + show logs"
DEFAULT_USER="${SUDO_USER:-}"
read -r -p "Enable tty1 auto-login and auto-start DriverTranslator logs at boot? (y/N): " AUTOLOG
AUTOLOG="${AUTOLOG:-N}"
if [[ "$AUTOLOG" =~ ^[Yy]$ ]]; then
  read -r -p "Console username to auto-login (default: ${DEFAULT_USER}): " CONSOLE_USER
  CONSOLE_USER="${CONSOLE_USER:-$DEFAULT_USER}"
  if [[ -z "$CONSOLE_USER" ]]; then
    echo "ERROR: No username provided for auto-login." >&2
    exit 1
  fi
  if ! id "$CONSOLE_USER" >/dev/null 2>&1; then
    echo "ERROR: User '$CONSOLE_USER' does not exist." >&2
    exit 1
  fi

  echo "Configuring getty@tty1 auto-login for '$CONSOLE_USER'..."
  mkdir -p /etc/systemd/system/getty@tty1.service.d
  cat >/etc/systemd/system/getty@tty1.service.d/override.conf <<EOF
[Service]
ExecStart=
ExecStart=-/sbin/agetty --autologin ${CONSOLE_USER} --noclear %I \$TERM
EOF

  echo "Installing login hook to auto-follow logs on tty1..."
  HOME_DIR="$(eval echo "~${CONSOLE_USER}")"
  PROFILE_FILE="${HOME_DIR}/.profile"
  MARKER_BEGIN="# >>> drivertranslator tty1 autolog (managed)"
  MARKER_END="# <<< drivertranslator tty1 autolog (managed)"

  # Remove any previous block we wrote
  if [[ -f "$PROFILE_FILE" ]]; then
    awk -v b="$MARKER_BEGIN" -v e="$MARKER_END" '
      $0==b {skip=1; next}
      $0==e {skip=0; next}
      !skip {print}
    ' "$PROFILE_FILE" >"${PROFILE_FILE}.tmp"
    mv "${PROFILE_FILE}.tmp" "$PROFILE_FILE"
  fi

  cat >>"$PROFILE_FILE" <<'EOF'
# >>> drivertranslator tty1 autolog (managed)
if [ -z "${SSH_TTY:-}" ] && [ "${DT_CONSOLE_LOGS:-1}" = "1" ] && [ "$(tty 2>/dev/null || true)" = "/dev/tty1" ]; then
  echo ""
  echo "DriverTranslator logs (Ctrl+C to exit to shell)."
  echo "Tip: set DT_CONSOLE_LOGS=0 to disable this for one session."
  echo ""
  journalctl -u drivertranslator.service -f -n 200
fi
# <<< drivertranslator tty1 autolog (managed)
EOF
  chown "$CONSOLE_USER:$CONSOLE_USER" "$PROFILE_FILE"

  systemctl daemon-reload
  systemctl restart getty@tty1.service

  echo "Enabled. On next boot, tty1 will auto-login and show live logs. Press Ctrl+C to reach a shell."
fi

echo "Done."

