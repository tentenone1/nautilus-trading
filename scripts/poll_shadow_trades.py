#!/usr/bin/env python3
"""Poll pending shadow trades and resolve their hypothetical P&L.

Runs on cron (every 15 min via hermes cron or system cron). For each
shadow_trades row with resolved=0, polls the Polymarket Gamma API to check
whether the market has closed. If resolved, computes hypothetical P&L and
writes it to the shadow_trades row.

Usage:
    python3 scripts/poll_shadow_trades.py [--limit N] [--dry-run]

Exit codes:
    0  — success
    1  — error
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

# Ensure project root on path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from strategies.wf_shadow_ledger import (
    backfill_sports_telemetry_signals,
    poll_pending_shadow_trades,
)
from strategies.wf_constants import SHADOW_TRADE_POLL_BATCH_SIZE

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("poll_shadow_trades")


def main() -> int:
    parser = argparse.ArgumentParser(description="Poll pending shadow trades for resolution")
    parser.add_argument(
        "--limit",
        type=int,
        default=SHADOW_TRADE_POLL_BATCH_SIZE,
        help=f"Max trades to poll per run (default: {SHADOW_TRADE_POLL_BATCH_SIZE})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Poll but don't write results to DB",
    )
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("poll_shadow_trades.py | starting poll | limit=%d | dry_run=%s", args.limit, args.dry_run)

    try:
        # Backfill any sports_telemetry signals not yet in shadow_trades
        # (handles signals logged before the ledger was active)
        log.info("Running backfill for existing sports_telemetry signals...")
        backfilled = backfill_sports_telemetry_signals()
        log.info("Backfill complete: %d new shadow_trade rows created", backfilled)
    except Exception as e:
        log.warning("Backfill failed (non-fatal): %s", e)

    try:
        if args.dry_run:
            log.info("Dry-run mode: polling skipped")
            return 0

        result = poll_pending_shadow_trades(limit=args.limit)

        log.info(
            "Poll complete | polled=%d resolved=%d pending=%d errors=%d | "
            "total_hypothetical_pnl=$%.2f",
            result["polled"],
            result["resolved"],
            result["pending"],
            result["errors"],
            result["total_hypothetical_pnl"],
        )

        if result["resolved"] > 0:
            log.info(
                "Resolved %d shadow trades | cumulative hypothetical P&L: $%.2f",
                result["resolved"],
                result["total_hypothetical_pnl"],
            )

    except Exception as e:
        log.error("Poll failed: %s", e, exc_info=True)
        return 1

    log.info("poll_shadow_trades.py | done")
    return 0


if __name__ == "__main__":
    sys.exit(main())
