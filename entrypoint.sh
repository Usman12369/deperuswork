#!/bin/sh
set -e

echo "=== Starting Deperus Bot ==="
echo "Current directory: $(pwd)"
echo "Python version: $(python --version)"
echo "Contents:"
ls -la

# Start health server in background
echo "Starting health server..."
python -c "
from health import run_health_server
run_health_server()
print('✅ Health server started')
" &

# Give health server time to start
sleep 3

# Start main bot
echo "Starting main bot..."
exec python bot.py