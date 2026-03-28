#!/usr/bin/env bash
set -euo pipefail

if [ ! -d .venv ]; then
  echo "Missing .venv. Run scripts/linux/setup.sh first."
  exit 1
fi

. .venv/bin/activate
python -m stream_place_capture --config config/streams.json
