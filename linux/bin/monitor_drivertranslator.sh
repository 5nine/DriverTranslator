#!/usr/bin/env bash
set -euo pipefail

SERVICE_NAME="drivertranslator.service"

usage() {
  cat <<'EOF'
monitor_drivertranslator.sh

Usage:
  ./linux/bin/monitor_drivertranslator.sh status
  ./linux/bin/monitor_drivertranslator.sh logs

Commands:
- status : show systemd status
- logs   : follow logs (journalctl -f)
EOF
}

cmd="${1:-}"
case "$cmd" in
  status)
    sudo systemctl --no-pager --full status "$SERVICE_NAME"
    ;;
  logs)
    sudo journalctl -u "$SERVICE_NAME" -f
    ;;
  -h|--help|"")
    usage
    ;;
  *)
    echo "Unknown command: $cmd" >&2
    usage
    exit 2
    ;;
esac

