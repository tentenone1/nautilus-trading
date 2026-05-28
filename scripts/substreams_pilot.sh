#!/bin/bash
# Run substreams for 24 hours (or until stopped)
# Stores L2 orderbook depth in PostgreSQL for comparison

set -e

SUBSTREAMS_DIR="/home/elon-1/projects/archived/polymarket-orderbook-substreams"
PG_CONN="postgres://postgres:postgres@localhost:5432/polymarket_substreams?sslmode=disable"
LOG_FILE="/home/elon-1/workspace/nautilus-trading/logs/substreams_pilot.log"
PID_FILE="/home/elon-1/workspace/nautilus-trading/logs/substreams_pilot.pid"

# Load token from hermes config
TOKEN=$(python3 -c "import yaml; print(yaml.safe_load(open('/home/elon-1/.hermes/config.yaml')).get('substreams', {}).get('api_token', ''))" 2>/dev/null)

if [ -z "$TOKEN" ]; then
    echo "[substreams-pilot] ERROR: No SUBSTREAMS_API_TOKEN found in ~/.hermes/config.yaml"
    exit 1
fi

echo "[substreams-pilot] Starting 24h pilot at $(date)"
echo "[substreams-pilot] Log: $LOG_FILE"
echo "[substreams-pilot] PID file: $PID_FILE"

cd "$SUBSTREAMS_DIR"

export SUBSTREAMS_API_TOKEN="$TOKEN"

nohup substreams-sink-sql run \
    "$PG_CONN" \
    https://spkg.io/PaulieB14/polymarket-orderbook-substreams-v0.4.0.spkg \
    -e polygon.substreams.pinax.network:443 \
    >> "$LOG_FILE" 2>&1 &

echo $! > "$PID_FILE"
echo "[substreams-pilot] Started with PID $(cat $PID_FILE)"
echo "[substreams-pilot] Monitor with: tail -f $LOG_FILE"
echo "[substreams-pilot] Stop with: kill \$(cat $PID_FILE)"
