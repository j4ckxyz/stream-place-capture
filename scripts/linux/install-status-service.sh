#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SERVICE_PATH="/etc/systemd/system/stream-place-capture-status.service"

cat > "$SERVICE_PATH" <<EOF
[Unit]
Description=Stream.place capture JSON status endpoint
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=$ROOT_DIR
ExecStart=$ROOT_DIR/.venv/bin/python -m stream_place_capture.status_server --config $ROOT_DIR/config/streams.json --host 0.0.0.0 --port 11456
Restart=always
RestartSec=3
Environment=PYTHONUNBUFFERED=1
StandardOutput=append:$ROOT_DIR/logs/status-service.out.log
StandardError=append:$ROOT_DIR/logs/status-service.err.log

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable --now stream-place-capture-status.service
systemctl is-active stream-place-capture-status.service
echo "Status endpoint running on port 11456"
