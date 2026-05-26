#!/bin/bash
# No-agent wrapper for Autoresearch Bridge cron
# This script runs bridge.py standalone (no LLM dependency)
# The output is delivered directly to Feishu by the cron system.
set -euo pipefail

NAUTILUS_DIR="/home/elon-1/workspace/nautilus-trading"
cd "$NAUTILUS_DIR"

export PYTHONPATH="$PYTHONPATH:."

# Run the bridge — it handles everything:
# 1. Fetch trades from last 5 minutes
# 2. Write bridge_data_{ts}.json
# 3. Run whale_perf_updater.py to close the feedback loop
exec python3 autoresearch-bridge/bridge.py
