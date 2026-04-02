#!/bin/sh
set -eu

echo "=== Starting bot container ==="
echo "Current directory: $(pwd)"
echo "Python version: $(python --version)"
echo "Contents:"
ls -la

echo "Starting shared health server on port ${PORT:-8080}..."
python -c "from health import run_health_server; run_health_server(port=int('${PORT:-8080}'))" &

echo "Starting bot supervisor..."
exec python run_bots.py
