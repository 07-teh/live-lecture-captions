#!/bin/zsh
# Live Lecture Captions — stop script (macOS). Double-click to run.
set -euo pipefail

APP_DIR="$(cd "$(dirname "$0")" && pwd)"
PID_FILE="$APP_DIR/backend.pid"

# Only stop processes running this folder's backend.py, never whatever else uses the port.
PIDS="$(pgrep -f "$APP_DIR/backend.py" || true)"
if [ -n "$PIDS" ]; then
  kill $PIDS >/dev/null 2>&1 || true
  echo "Backend stopped."
else
  echo "Backend was not running."
fi
rm -f "$PID_FILE"
