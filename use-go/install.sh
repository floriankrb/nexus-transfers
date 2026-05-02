#!/usr/bin/env bash
# install.sh — Builds and installs the relay server on the relay host.
# Run as root (or with sudo).
#
# Usage: ./install.sh <domain>

set -euo pipefail

DOMAIN="${1:-}"
if [[ -z "$DOMAIN" ]]; then
    echo "Usage: $0 <domain>"
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# 1. Build relay binary
echo "==> Building relay..."
mkdir -p /opt/relay
cp "$SCRIPT_DIR/relay/main.go" /opt/relay/main.go
cp "$SCRIPT_DIR/relay/go.mod"  /opt/relay/go.mod
(cd /opt/relay && go build -o relay-bin .)
echo "    /opt/relay/relay-bin built."

# 2. Install systemd service
echo "==> Installing systemd service..."
cp "$SCRIPT_DIR/relay.service" /etc/systemd/system/relay.service
systemctl daemon-reload
systemctl enable --now relay
systemctl status relay --no-pager

# 3. Configure Nginx (review + manual reload required)
echo "==> Configuring Nginx..."
bash "$SCRIPT_DIR/setup-nginx.sh" "$DOMAIN"

echo ""
echo "Done. After reviewing the Nginx config, run:"
echo "  nginx -t && systemctl reload nginx"
