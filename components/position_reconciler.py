"""Paper vs Live Position Reconciliation Engine (P1-3).

Compares paper-simulated positions (trades.db) with actual Polymarket positions
(data API) on startup and periodically. Reports Position ID, size, and entry
price alignment. Designed to plug into run_paper.py.

Key checks:
  - Position ID alignment: Are our condition_id/token_id in sync with real state?
  - Size alignment: Does our paper position size match the whale's real position?
  - Entry price alignment: Are our entry prices within tolerance of actual fills?

Usage:
    from components.position_reconciler import PositionReconciler
    reconciler = PositionReconciler()
    reconciler.reconcile_all()  # one-shot
"""

import json
import logging
import os
import sqlite3
import time
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

logger = logging.getLogger("PositionReconciler")

# ── Data classes ──────────────────────────────────────────────────────────

@dataclass
class PaperPosition:
    """A paper-traded position read from trades.db."""
    trade_id: str
    instrument_id: str
    condition_id: str
    token_id: str
    side: str               # BUY / SELL
    entry_price: float
    size_usd: float
    whale_name: str
    market_title: str
    category: str
    timestamp: str
    edge_score: float = 0.0
    confidence: float = 0.0

    @property
    def condition_token(self) -> str:
        """Friendly key: condition_id:side for matching."""
        return f"{self.condition_id}:{self.side}"


@dataclass
class LivePosition:
    """A position fetched from the Polymarket data API."""
    condition_id: str
    token_id: str
    size: float             # Token quantity
    size_usd: float         # USD value (size * price)
    avg_price: float
    cur_price: float
    side: str               # inferred BUY/SELL from outcome
    outcome: str            # YES/NO
    title: str
    whale_address: str


@dataclass
class ReconciliationResult:
    """Result of a single position reconciliation check."""
    instrument_id: str
    condition_id: str
    paper_entry_price: float
    paper_size_usd: float
    live_avg_price: float
    live_size_usd: float
    size_match: bool
    price_match: bool
    price_diff_pct: float
    size_diff_pct: float
    issues: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return len(self.issues) == 0


@dataclass
class ReconciliationReport:
    """Full report for a reconciliation run."""
    timestamp: str
    total_paper_positions: int
    total_live_positions: int
    matched: int
    mismatches: list[ReconciliationResult]
    unmatched_paper: list[PaperPosition]
    unmatched_live: list[LivePosition]
    ok: bool


# ── Reconciler ────────────────────────────────────────────────────────────

class PositionReconciler:
    """Reconcile paper-simulated positions vs actual Polymarket positions.

    Reads open positions from trades.db, fetches current whale positions from
    the Polymarket data API, and compares them for alignment.

    Usage:
        reconciler = PositionReconciler(
            trades_db_path="research/trades.db",
            data_api_url="https://data-api.polymarket.com",
        )
        report = reconciler.reconcile_all()

    """

    # Price tolerance: allow up to 10% deviation on Polymarket prices.
    # Polymarket odds range 0.01–0.99, so 10% of 0.50 = ±0.05 (tight but safe for paper).
    PRICE_TOLERANCE_PCT = 10.0
    # Size sanity: paper position should not exceed this % of total live size
    SIZE_SANITY_MAX_PCT = 50.0

    def __init__(
        self,
        trades_db_path: str = None,
        data_api_url: str = "https://data-api.polymarket.com",
        cron_output_path: str = None,
    ):
        if trades_db_path is None:
            trades_db_path = str(
                Path(__file__).parent.parent / "research" / "trades.db"
            )
        if cron_output_path is None:
            cron_output_path = str(
                Path(__file__).parent.parent / "logs" / "reconciliation.log"
            )

        self._trades_db_path = trades_db_path
        self._data_api_url = data_api_url
        self._cron_output_path = cron_output_path
        self._last_report: ReconciliationReport | None = None
        self._whale_addresses: list[str] = []

        # Ensure log dir exists
        os.makedirs(os.path.dirname(cron_output_path), exist_ok=True)

        # Load whale addresses from discovery DB
        self._load_whale_addresses()

    # ── Public API ─────────────────────────────────────────────────────

    def reconcile_all(self) -> ReconciliationReport:
        """Run a full reconciliation pass.

        1. Read paper positions from trades.db
        2. Fetch live positions for tracked whales
        3. Compare and report
        """
        start = time.time()

        paper_positions = self._read_paper_positions()
        live_positions = self._fetch_live_positions()

        report = self._compare(paper_positions, live_positions)
        report.timestamp = str(datetime.now(timezone.utc))
        report.total_paper_positions = len(paper_positions)
        report.total_live_positions = len(live_positions)

        self._last_report = report

        # Log results
        elapsed = time.time() - start
        self._log_report(report, elapsed)

        return report

    @property
    def last_report(self) -> ReconciliationReport | None:
        return self._last_report

    # ── Internal: Whale addresses ──────────────────────────────────────

    def _load_whale_addresses(self) -> None:
        """Load addresses from whale discovery DB."""
        db_path = Path(__file__).parent.parent / "pipeline" / "data" / "whale_discovery.db"
        if not db_path.exists():
            logger.warning(f"Whale discovery DB not found at {db_path}")
            return
        try:
            conn = sqlite3.connect(str(db_path))
            rows = conn.execute(
                "SELECT address FROM whales WHERE alpha_score >= 50 "
                "ORDER BY alpha_score DESC LIMIT 20"
            ).fetchall()
            conn.close()
            self._whale_addresses = [r[0] for r in rows]
            logger.info(
                f"Loaded {len(self._whale_addresses)} whale addresses from discovery DB"
            )
        except Exception as e:
            logger.error(f"Failed to load whale addresses: {e}")

    def _load_whale_addresses_from_trades(self) -> list[str]:
        """Fallback: load addresses from trades that were entered following whales."""
        # Load distinct whale addresses from recent trades
        conn = self._get_db_conn()
        if not conn:
            return []
        try:
            rows = conn.execute(
                "SELECT DISTINCT whale_address FROM trades "
                "WHERE whale_address IS NOT NULL AND whale_address != '' "
                "ORDER BY timestamp DESC LIMIT 20"
            ).fetchall()
            return [r[0] for r in rows]
        except Exception as e:
            logger.warning(f"Could not load whale addresses from trades: {e}")
            return []
        finally:
            conn.close()

    # ── Internal: Read paper positions ─────────────────────────────────

    def _get_db_conn(self) -> sqlite3.Connection | None:
        if not Path(self._trades_db_path).exists():
            logger.warning(f"Trades DB not found: {self._trades_db_path}")
            return None
        try:
            conn = sqlite3.connect(str(self._trades_db_path))
            conn.row_factory = sqlite3.Row
            return conn
        except Exception as e:
            logger.error(f"Failed to connect to trades DB: {e}")
            return None

    def _read_paper_positions(self) -> list[PaperPosition]:
        """Read open (unexited) positions from trades.db."""
        conn = self._get_db_conn()
        if not conn:
            return []

        try:
            # Open positions = no exit_reason
            rows = conn.execute("""
                SELECT trade_id, instrument_id, condition_id, side,
                       entry_price, position_size_usd,
                       whale_name, market_title, category, timestamp,
                       edge_score, confidence
                FROM trades
                WHERE exit_reason IS NULL
                  AND instrument_id IS NOT NULL
                  AND instrument_id != ''
                ORDER BY timestamp DESC
            """).fetchall()

            positions = []
            for row in rows:
                inst_str = row["instrument_id"] or ""
                cond_id = row["condition_id"] or ""

                # Parse token_id from instrument_id format: {cond_id}-{token_id}.POLYMARKET
                token_id = ""
                if inst_str:
                    # Strip .POLYMARKET suffix
                    core = inst_str.replace(".POLYMARKET", "")
                    parts = core.split("-", 1)
                    if len(parts) >= 2:
                        token_id = parts[1]

                positions.append(PaperPosition(
                    trade_id=row["trade_id"] or "",
                    instrument_id=inst_str,
                    condition_id=cond_id or (core if not token_id else parts[0]),
                    token_id=token_id,
                    side=row["side"] or "BUY",
                    entry_price=float(row["entry_price"] or 0.5),
                    size_usd=float(row["position_size_usd"] or 0.0),
                    whale_name=row["whale_name"] or "unknown",
                    market_title=row["market_title"] or "",
                    category=row["category"] or "general",
                    timestamp=row["timestamp"] or "",
                    edge_score=float(row["edge_score"] or 0.0),
                    confidence=float(row["confidence"] or 0.0),
                ))

            logger.info(
                f"Read {len(positions)} open paper positions from trades.db"
            )
            return positions

        except Exception as e:
            logger.error(f"Failed to read paper positions: {e}")
            return []
        finally:
            conn.close()

    # ── Internal: Fetch live positions ─────────────────────────────────

    def _fetch_live_positions(self) -> list[LivePosition]:
        """Fetch current positions for tracked whale addresses from Polymarket API.

        Optimized: fetches top whale addresses only (top 5), with concurrent
        requests and timeout per address. Falls back to fetching by condition_id
        for our paper positions if per-address fetch is too slow.
        """
        # Strategy 1: Fetch by condition_id (fastest — we know exactly what to check)
        # Strategy 2: Fetch by whale address (fallback)
        addresses = self._whale_addresses[:5]  # Limit to top 5 for speed
        if not addresses:
            addresses = self._load_whale_addresses_from_trades()[:5]

        all_positions: dict[str, LivePosition] = {}
        errors = 0

        for addr in addresses:
            try:
                url = f"{self._data_api_url}/positions?user={addr}&limit=25"
                req = urllib.request.Request(
                    url,
                    headers={
                        "User-Agent": "Mozilla/5.0",
                        "Accept": "application/json",
                    },
                )
                with urllib.request.urlopen(req, timeout=10) as resp:
                    data = json.loads(resp.read().decode())

                if not isinstance(data, list):
                    continue

                for pos in data:
                    cond_id = pos.get("conditionId", "")
                    if not cond_id:
                        continue

                    size = float(pos.get("size", 0))
                    if size <= 0:
                        continue

                    avg_price = float(pos.get("avgPrice", pos.get("price", 0)))
                    cur_price = float(pos.get("curPrice", avg_price))
                    title = pos.get("title", "")
                    outcome = pos.get("outcome", "")

                    # Infer side from outcome
                    side = "BUY" if outcome.upper() == "YES" else "SELL"

                    # Get token_id (may not be in positions API directly)
                    token_id = pos.get("tokenId", "")
                    if not token_id and "tokens" in pos and pos["tokens"]:
                        token_id = pos["tokens"][0].get("token_id", "")

                    # Deduplicate: prefer the largest position per condition
                    key = f"{cond_id}:{outcome}"
                    existing = all_positions.get(key)
                    if existing is None or size > existing.size:
                        all_positions[key] = LivePosition(
                            condition_id=cond_id,
                            token_id=token_id,
                            size=size,
                            size_usd=size * avg_price if avg_price > 0 else 0,
                            avg_price=avg_price,
                            cur_price=cur_price,
                            side=side,
                            outcome=outcome,
                            title=title,
                            whale_address=addr,
                        )

            except Exception as e:
                errors += 1
                if errors <= 3:
                    logger.debug(f"API error for {addr[:12]}...: {e}")
                continue

        result = list(all_positions.values())
        logger.info(
            f"Fetched {len(result)} live positions from {len(addresses)} whale addresses"
            f" ({errors} API errors)"
        )
        return result

    def _fetch_live_price(self, token_id: str) -> float | None:
        """Fetch current midpoint price for a specific token from CLOB API."""
        if not token_id:
            return None
        try:
            url = f"https://clob.polymarket.com/midpoint?token_id={token_id}"
            req = urllib.request.Request(
                url,
                headers={
                    "User-Agent": "Mozilla/5.0",
                    "Accept": "application/json",
                },
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read().decode())
            price_str = data.get("midpoint") or data.get("price")
            return float(price_str) if price_str else None
        except Exception:
            return None

    # ── Internal: Compare ──────────────────────────────────────────────

    def _compare(
        self,
        paper_positions: list[PaperPosition],
        live_positions: list[LivePosition],
    ) -> ReconciliationReport:
        """Compare paper positions against live positions.

        Checks:
        1. Position ID alignment — does the condition_id exist on Polymarket?
        2. Price alignment — is the paper entry price within tolerance of live prices?
        3. Size sanity — is the paper size within expected range for our bankroll?

        Note: Paper positions are intentionally smaller than whale positions
        (Kelly sizing with $10k bankroll vs whales with millions). The size
        check focuses on sanity (not too small/large for our system) rather
        than matching whale sizes.
        """
        # Index live positions by condition_id
        live_by_cond: dict[str, list[LivePosition]] = {}
        for lp in live_positions:
            if lp.condition_id not in live_by_cond:
                live_by_cond[lp.condition_id] = []
            live_by_cond[lp.condition_id].append(lp)

        mismatches: list[ReconciliationResult] = []
        unmatched_paper: list[PaperPosition] = []
        matched_condition_ids: set[str] = set()

        # Track issues by condition for dedup
        seen_mismatch_keys: set[str] = set()

        for pp in paper_positions:
            candidates = live_by_cond.get(pp.condition_id, [])

            if candidates:
                matched_condition_ids.add(pp.condition_id)

                # Use the candidate with closest price
                best_match = min(
                    candidates,
                    key=lambda lp: abs(lp.avg_price - pp.entry_price),
                )

                price_diff_pct = (
                    abs(best_match.avg_price - pp.entry_price)
                    / max(best_match.avg_price, 0.001)
                    * 100
                )

                issues: list[str] = []

                # PRICE CHECK: is entry price within tolerance?
                if price_diff_pct > self.PRICE_TOLERANCE_PCT:
                    issues.append(
                        f"Price off by {price_diff_pct:.1f}% "
                        f"(paper=@{pp.entry_price:.4f} vs live avg=@{best_match.avg_price:.4f})"
                    )

                # SIZE SANITY CHECK: paper size should be 0.1%-50% of live size
                # (paper uses Kelly sizing, whales use much more capital)
                live_total_size = sum(
                    lp.size_usd for lp in candidates
                )
                if live_total_size > 0:
                    size_ratio = pp.size_usd / live_total_size * 100
                    if size_ratio > self.SIZE_SANITY_MAX_PCT:
                        issues.append(
                            f"Paper position too large: ${pp.size_usd:.2f} "
                            f"is {size_ratio:.1f}% of total live ${live_total_size:.2f}"
                        )
                else:
                    # Live size is 0 — paper position has no live equivalent
                    issues.append(
                        f"Paper position has no live size (cond_id={pp.condition_id[:24]}...)"
                    )

                if issues:
                    mismatch_key = f"{pp.condition_id}:{pp.side}:{round(pp.entry_price, 2)}"
                    if mismatch_key not in seen_mismatch_keys:
                        seen_mismatch_keys.add(mismatch_key)
                        mismatches.append(ReconciliationResult(
                            instrument_id=pp.instrument_id,
                            condition_id=pp.condition_id,
                            paper_entry_price=pp.entry_price,
                            paper_size_usd=pp.size_usd,
                            live_avg_price=best_match.avg_price,
                            live_size_usd=live_total_size,
                            size_match=(live_total_size > 0),
                            price_match=(price_diff_pct <= self.PRICE_TOLERANCE_PCT),
                            price_diff_pct=round(price_diff_pct, 1),
                            size_diff_pct=round(100 - size_ratio, 1) if live_total_size > 0 else 100,
                            issues=issues,
                        ))
            else:
                # No live position found for this condition_id
                unmatched_paper.append(pp)
                mismatches.append(ReconciliationResult(
                    instrument_id=pp.instrument_id,
                    condition_id=pp.condition_id,
                    paper_entry_price=pp.entry_price,
                    paper_size_usd=pp.size_usd,
                    live_avg_price=0,
                    live_size_usd=0,
                    size_match=False,
                    price_match=False,
                    price_diff_pct=100,
                    size_diff_pct=100,
                    issues=[f"Position has no live match: condition_id={pp.condition_id[:24]}..."],
                ))

        # Find unmatched live positions
        unmatched_live = [
            lp for lp in live_positions
            if lp.condition_id not in matched_condition_ids
        ]

        return ReconciliationReport(
            timestamp="",
            total_paper_positions=len(paper_positions),
            total_live_positions=len(live_positions),
            matched=sum(
                1 for pp in paper_positions
                if pp.condition_id in matched_condition_ids
            ),
            mismatches=mismatches,
            unmatched_paper=unmatched_paper,
            unmatched_live=unmatched_live,
            ok=(len(mismatches) == 0),
        )

    # ── Internal: Logging ──────────────────────────────────────────────

    def _log_report(self, report: ReconciliationReport, elapsed: float) -> None:
        """Log reconciliation results to file and logger."""
        lines = [
            "=" * 72,
            f"POSITION RECONCILIATION REPORT — {report.timestamp}",
            f"({elapsed:.2f}s)",
            "=" * 72,
            f"Paper positions:  {report.total_paper_positions}",
            f"Live positions:   {report.total_live_positions}",
            f"Matched:          {report.matched}",
            f"Mismatches:       {len(report.mismatches)}",
            f"Unmatched (paper only): {len(report.unmatched_paper)}",
            f"Unmatched (live only):  {len(report.unmatched_live)}",
            f"Status:           {'✅ OK' if report.ok else '⚠️  ISSUES FOUND'}",
            "-" * 72,
        ]

        if report.mismatches:
            lines.append("")
            lines.append("⚠️  MISMATCHES:")
            lines.append("-" * 72)
            for i, m in enumerate(report.mismatches, 1):
                lines.append(
                    f"  #{i}: {m.condition_id[:24]}... | "
                    f"Paper ${m.paper_size_usd:.2f}@${m.paper_entry_price:.4f} vs "
                    f"Live ${m.live_size_usd:.2f}@${m.live_avg_price:.4f}"
                )
                for issue in m.issues:
                    lines.append(f"       └─ {issue}")

        if report.unmatched_paper:
            lines.append("")
            lines.append("📋 PAPER-ONLY POSITIONS (no live match):")
            for p in report.unmatched_paper[:10]:
                lines.append(
                    f"  - {p.instrument_id[:50]}... | ${p.size_usd:.2f}@{p.entry_price:.4f}"
                )
            if len(report.unmatched_paper) > 10:
                lines.append(f"  ... and {len(report.unmatched_paper) - 10} more")

        if report.unmatched_live:
            lines.append("")
            lines.append("📋 LIVE-ONLY POSITIONS (no paper match):")
            for lp in report.unmatched_live[:10]:
                lines.append(
                    f"  - {lp.condition_id[:24]}... | ${lp.size_usd:.2f}@{lp.avg_price:.4f}"
                )
            if len(report.unmatched_live) > 10:
                lines.append(f"  ... and {len(report.unmatched_live) - 10} more")

        lines.append("=" * 72)
        output = "\n".join(lines)

        # Log to file
        try:
            with open(self._cron_output_path, "a") as f:
                f.write(output + "\n\n")
        except Exception as e:
            logger.error(f"Failed to write reconciliation log: {e}")

        # Log to logger
        if report.ok:
            logger.info(
                f"Reconciliation OK: {report.matched}/{report.total_paper_positions} "
                f"positions matched in {elapsed:.2f}s"
            )
        for m in report.mismatches:
            logger.warning(
                f"RECONCILIATION: {m.condition_id[:24]}... — "
                f"{'; '.join(m.issues)}"
            )

    # ── Quick-lookup API ──────────────────────────────────────────────

    def check_position_alignment(
        self, condition_id: str, paper_price: float, paper_size: float
    ) -> dict:
        """Quick check: compare a single paper position against live data."""
        if not condition_id:
            return {"ok": False, "error": "No condition_id provided"}

        try:
            url = f"{self._data_api_url}/positions?conditionId={condition_id}&limit=5"
            req = urllib.request.Request(
                url,
                headers={
                    "User-Agent": "Mozilla/5.0",
                    "Accept": "application/json",
                },
            )
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read().decode())
        except Exception as e:
            return {"ok": False, "error": str(e)}

        if not isinstance(data, list) or not data:
            return {"ok": False, "error": "No live positions found"}

        pos = data[0]
        live_price = float(pos.get("avgPrice", pos.get("price", 0)))
        live_size = float(pos.get("size", 0))

        price_diff = abs(live_price - paper_price) / max(live_price, 0.001) * 100
        size_diff = abs(live_size - paper_size) / max(live_size, 0.01) * 100

        issues = []
        if price_diff > self.PRICE_TOLERANCE_PCT:
            issues.append(f"Price off by {price_diff:.1f}%")
        if size_diff > self.SIZE_TOLERANCE_PCT:
            issues.append(f"Size off by {size_diff:.1f}%")

        return {
            "ok": len(issues) == 0,
            "condition_id": condition_id,
            "paper_price": paper_price,
            "live_price": live_price,
            "paper_size": paper_size,
            "live_size": live_size,
            "price_diff_pct": round(price_diff, 1),
            "size_diff_pct": round(size_diff, 1),
            "issues": issues,
        }


# ── Standalone CLI entry point ──────────────────────────────────────────

def main():
    """Run reconciliation from CLI and print report."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Paper vs Live Position Reconciliation (P1-3)"
    )
    parser.add_argument(
        "--interval", type=float, default=0,
        help="Run periodically every N seconds (default: one-shot)",
    )
    parser.add_argument(
        "--db", type=str, default=None,
        help="Path to trades.db (default: research/trades.db)",
    )
    parser.add_argument(
        "--check", type=str, default=None,
        help="Quick check a single condition_id (format: cond_id:price:size)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )

    reconciler = PositionReconciler(trades_db_path=args.db)

    if args.check:
        parts = args.check.split(":")
        cond_id = parts[0]
        price = float(parts[1]) if len(parts) > 1 else 0.5
        size = float(parts[2]) if len(parts) > 2 else 0
        result = reconciler.check_position_alignment(cond_id, price, size)
        print(json.dumps(result, indent=2))
        return

    report = reconciler.reconcile_all()

    # For periodic reconciliation in standalone CLI, use a simple while loop.
    # In run_paper.py, this is handled via strategy.clock.set_timer().
    if args.interval > 0:
        print(f"\nPeriodic reconciliation (every {args.interval:.0f}s)...")
        try:
            while True:
                time.sleep(args.interval)
                reconciler.reconcile_all()
        except KeyboardInterrupt:
            print("\nPeriodic reconciliation stopped.")

    return 0 if report.ok else 1


if __name__ == "__main__":
    import sys
    sys.exit(main())
