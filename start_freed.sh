#!/bin/zsh
# FREED launch wrapper — ensures API key is loaded and valid before spawning daemon.
# Use this instead of calling python3 freed.py directly.
# Usage: ./start_freed.sh

source ~/.zshrc

if [[ -z "$ANTHROPIC_API_KEY" ]]; then
    echo "[FREED] ERROR: ANTHROPIC_API_KEY not set. Check ~/.zshrc"
    exit 1
fi

if [[ ! "$ANTHROPIC_API_KEY" == sk-ant-* ]]; then
    echo "[FREED] ERROR: ANTHROPIC_API_KEY looks malformed (expected 'sk-ant-...')"
    echo "  Got: ${ANTHROPIC_API_KEY:0:12}..."
    exit 1
fi

echo "[FREED] API key OK (${ANTHROPIC_API_KEY:0:14}...)"
cd "$(dirname "$0")"

DEV_FLAG=""
while true; do
    echo ""
    echo "Run mode?"
    echo "  1. Real cycle (operational budget)"
    echo "  2. Dev run   (budget caps disabled)"
    echo -n "Choice [1]: "
    read choice
    case "$choice" in
        ""|1) break ;;
        2|dev|DEV) DEV_FLAG="--dev"; break ;;
        *) echo "Enter 1 or 2." ;;
    esac
done

if [[ -n "$DEV_FLAG" ]]; then
    echo "[FREED] DEV MODE — budget caps disabled."
else
    echo "[FREED] REAL cycle — operational budget active."
fi
echo ""

mkdir -p FREED_log
LOGFILE="FREED_log/stdout_$(date +%Y%m%d).log"

nohup caffeinate -i python3 -u freed.py $DEV_FLAG "$@" >> "$LOGFILE" 2>&1 &
DAEMON_PID=$!
disown

sleep 2
if kill -0 $DAEMON_PID 2>/dev/null; then
    echo "[FREED] Daemon launched in background (PID $DAEMON_PID)."
    echo "[FREED] Log:  $LOGFILE"
    echo "[FREED] Tail: tail -f $LOGFILE"
    echo "[FREED] Stop: kill \$(cat freed.pid)"
else
    echo "[FREED] ERROR: daemon failed to start within 2s. Last log lines:"
    tail -30 "$LOGFILE"
    exit 1
fi
