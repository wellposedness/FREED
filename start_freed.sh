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
exec caffeinate -i python3 -u freed.py "$@"
