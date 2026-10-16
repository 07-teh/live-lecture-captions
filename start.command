#!/bin/zsh
# Live Lecture Captions — start script (macOS). Double-click to run.
# The first run creates a Python virtual environment and installs dependencies.
set -euo pipefail

APP_DIR="$(cd "$(dirname "$0")" && pwd)"
PORT=8081
LOG_FILE="$APP_DIR/backend.log"
PID_FILE="$APP_DIR/backend.pid"

cd "$APP_DIR"

if lsof -nP -iTCP:$PORT -sTCP:LISTEN >/dev/null 2>&1; then
  echo "Something is already running on port $PORT. Opening it."
  open "http://127.0.0.1:$PORT"
  exit 0
fi

if ! command -v python3 >/dev/null 2>&1 || ! python3 -c 'import sys; sys.exit(sys.version_info < (3, 10))'; then
  echo "Python 3.10 or newer is required."
  echo "Install it from https://www.python.org/downloads/  (or: brew install python)"
  read -r "?Press Enter to close..."
  exit 1
fi

if [ ! -x "$APP_DIR/venv/bin/python" ]; then
  echo "First run: setting up the Python environment (one time)..."
  python3 -m venv venv
  "$APP_DIR/venv/bin/python" -m pip install --upgrade pip >/dev/null
  "$APP_DIR/venv/bin/pip" install -r "$APP_DIR/requirements.txt"
fi

if [ ! -f "$APP_DIR/.env" ]; then
  cp "$APP_DIR/.env.example" "$APP_DIR/.env"
  echo "Created .env from .env.example."
  echo ">> Open .env, set STT_API_KEY (Deepgram) and DEEPSEEK_API_KEY (DeepSeek), then run stop.command and start.command again."
fi

nohup "$APP_DIR/venv/bin/python" "$APP_DIR/backend.py" > "$LOG_FILE" 2>&1 &
echo "$!" > "$PID_FILE"

for _ in {1..30}; do
  if curl -fsS "http://127.0.0.1:$PORT/health" >/dev/null 2>&1; then
    open "http://127.0.0.1:$PORT"
    echo "Started. Backend log: $LOG_FILE"
    exit 0
  fi
  sleep 1
done

echo "Failed to start. Recent log:"
tail -n 20 "$LOG_FILE" || true
read -r "?Press Enter to close..."
exit 1
