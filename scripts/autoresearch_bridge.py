#!/usr/bin/env python3
"""Autoresearch Bridge — takes signal monitor detections and produces trade recommendations.

Pipeline:
1. Reads signal_detections.json for tracked markets
2. For each promising detection, queries CLOB midpoint for live pricing
3. Feeds market data through uncensored LLM for analysis
4. Outputs trade card: BUY/WAIT/SKIP with entry, target, stop, Kelly

Output: research/trade_recommendations.json
State: research/autoresearch_state.json
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sqlite3
import sys
import time
import urllib.request
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
PROJECT_ROOT: Path = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from components.bitable_writer import write_research_log

logger = logging.getLogger("autoresearch_bridge")

# ─── Constants ───────────────────────────────────────────────────────────────

POLL_INTERVAL_SECS: int = 300
LLM_TIMEOUT_SECS: int = 60
API_TIMEOUT_SECS: int = 10
RUN_TIMEOUT_SECS: int = 110  # Max wall-clock time for single pass (10s buffer for 120s cron)

PROJECT_ROOT: Path = Path(__file__).resolve().parent.parent
STATE_FILE: Path = PROJECT_ROOT / "research" / "autoresearch_state.json"
DETECTIONS_FILE: Path = PROJECT_ROOT / "research" / "signal_detections.json"
OUTPUT_FILE: Path = PROJECT_ROOT / "research" / "trade_recommendations.json"
WHALE_DB_PATH: Path = PROJECT_ROOT / "data" / "whale_discovery.db"

LLM_URL: str = "http://127.0.0.1:8080/v1/chat/completions"
LLM_MODEL: str = "qwen3.6-35b-a3b-uncensored-hauhaucs-aggressive"
CLOB_MIDPOINT_URL: str = "https://clob.polymarket.com/midpoint?token_id={}"
MARKETS_API: str = "https://data-api.polymarket.com/markets?conditionId={}&limit=1"

NOISE_TITLES: list[str] = ["highest temperature", "Bitcoin Up or Down", "Ethereum Up or Down", "Solana Up or Down"]

# ─── Quality Gates ─────────────────────────────────────────────────────────────
KELLY_MIN = 0.01            # 1% floor
KELLY_MAX = 0.125           # 12.5% ceiling (matches whale_tiers.json max_position_pct)
CONFIDENCE_MIN = 0.65       # Minimum confidence for actionable signal


class BridgeError(Exception):
    """Error in autoresearch bridge operations."""


@dataclass
class BridgeState:
    """Persistent state for tracking processed detections."""
    processed_timestamps: dict[str, str] = field(default_factory=dict)
    last_run: str = ""

    @classmethod
    def load(cls, path: Path) -> BridgeState:
        """Load state from JSON file. Returns empty state if missing."""
        if not path.exists():
            return cls()
        try:
            data = json.loads(path.read_text())
            return cls(
                processed_timestamps=data.get("processed_timestamps", {}),
                last_run=data.get("last_run", ""),
            )
        except json.JSONDecodeError as exc:
            logger.warning("Invalid state file %s: %s", path, exc, extra={"path": str(path)})
            return cls()

    def save(self, path: Path) -> None:
        """Save state to JSON file."""
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({
            "processed_timestamps": self.processed_timestamps,
            "last_run": self.last_run,
        }, indent=2))

    def is_processed(self, detection_key: str) -> bool:
        """Check if detection key has been processed."""
        return detection_key in self.processed_timestamps

    def mark_processed(self, detection_key: str, timestamp: str) -> None:
        """Mark a detection as processed."""
        self.processed_timestamps[detection_key] = timestamp


_whale_cache: dict[str, list[dict]] = {}


def fetch_json(url: str, timeout: int = API_TIMEOUT_SECS) -> Optional[dict | list]:
    """Fetch JSON from URL with error handling."""
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read())
    except Exception as exc:
        logger.debug("Fetch failed for %s: %s", url, exc, extra={"url": url})
        return None


def query_llm(prompt: str) -> str:
    """Query the LLM with a prompt and return the response."""
    payload = json.dumps({
        "model": LLM_MODEL,
        "messages": [
            {"role": "system", "content": "You are a Polymarket trading analyst. Output ONLY a single JSON object with no preamble, no thinking, no explanation. Start your response with { and end with }. Fields: market, decision (BUY/WAIT/SKIP), confidence (0.0-1.0), reason, entry_price, target_price, stop_price, kelly_fraction, hold_hours."},
            {"role": "user", "content": prompt}
        ],
        "max_tokens": 8192,
        "temperature": 0.0
    }).encode()
    try:
        req = urllib.request.Request(LLM_URL, data=payload, headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=LLM_TIMEOUT_SECS) as resp:
            data = json.loads(resp.read())
        msg = data["choices"][0]["message"]
        content = msg.get("content", "") or ""
        return content
    except Exception as exc:
        logger.error("LLM query failed: %s", exc, extra={"error": str(exc)})
        return f"LLM error: {exc}"


def get_market_info(condition_id: str) -> Optional[dict]:
    """Fetch market info from Polymarket Data API."""
    if not condition_id:
        return None
    data = fetch_json(MARKETS_API.format(condition_id))
    if data and isinstance(data, list) and len(data) > 0:
        return data[0]
    return None


def check_midpoint(condition_id: str) -> dict[str, float]:
    """Query CLOB API to get midpoint prices for all tokens in a market.
    
    Args:
        condition_id: The condition ID to look up.
        
    Returns:
        Dict mapping outcome name to midpoint price. Empty dict on failure.
    """
    if not condition_id:
        return {}
    
    # Step 1: Get token IDs from market info
    market_info = get_market_info(condition_id)
    if not market_info:
        logger.debug("No market info for condition_id: %s", condition_id, extra={"condition_id": condition_id})
        return {}
    
    tokens = market_info.get("tokens", []) or market_info.get("outcomes", [])
    if not tokens:
        # Try to extract from outcomes array
        outcomes = market_info.get("outcomes", [])
        if isinstance(outcomes, list):
            tokens = outcomes
    
    results: dict[str, float] = {}
    
    for token in tokens:
        if isinstance(token, dict):
            token_id = token.get("token_id") or token.get("clobTokenIds", [None])[0] if token.get("clobTokenIds") else None
            outcome = token.get("outcome") or token.get("name", "unknown")
        else:
            # Token might just be a string ID
            token_id = token
            outcome = f"outcome_{len(results)}"
        
        if not token_id:
            continue
            
        # Step 2: Query midpoint for each token
        midpoint_data = fetch_json(CLOB_MIDPOINT_URL.format(token_id))
        if midpoint_data and isinstance(midpoint_data, dict):
            # Midpoint API returns {"midpoint": "0.55"} or similar
            midpoint_str = midpoint_data.get("midpoint") or midpoint_data.get("price")
            if midpoint_str:
                try:
                    results[outcome] = float(midpoint_str)
                    logger.debug("Midpoint for %s: %.4f", outcome, results[outcome], extra={"outcome": outcome, "price": results[outcome]})
                except (TypeError, ValueError):
                    pass
    
    return results


def lookup_whales(condition_id: str) -> list[dict]:
    """Look up whale signals for a condition from the whale discovery database."""
    if not condition_id or not WHALE_DB_PATH.exists():
        return []
    
    cid = condition_id.strip().lower()
    if cid in _whale_cache:
        return _whale_cache[cid]
    
    try:
        conn = sqlite3.connect(WHALE_DB_PATH)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT ws.whale_name, ws.usd_value, ws.side, ws.outcome, 
                   wi.classification, wi.trust_score
            FROM whale_signals ws
            LEFT JOIN whale_intelligence wi ON ws.whale_address = wi.address
            WHERE LOWER(ws.condition_id)=? AND ws.usd_value>=1000
            ORDER BY ws.usd_value DESC
            LIMIT 10
            """,
            (cid,)
        ).fetchall()
        conn.close()
        result = [dict(r) for r in rows]
        _whale_cache[cid] = result
        return result
    except Exception as exc:
        logger.warning("Whale lookup failed: %s", exc, extra={"condition_id": condition_id})
        return []


def analyze_market(
    detection: dict,
    market_info: Optional[dict] = None,
    midpoints: Optional[dict] = None,
) -> dict:
    """Analyze a market detection and return a trade recommendation.
    
    Args:
        detection: Signal detection dict with market name, price, age.
        market_info: Polymarket market metadata (category, resolution date).
        midpoints: Current CLOB midpoint prices for outcomes.
    
    Returns:
        Trade recommendation dict with decision, confidence, prices, Kelly.
    """
    market = detection.get("market", "Unknown")
    price = detection.get("entry_price") or detection.get("lowest_price", 0.5)
    age = detection.get("age_seconds", 0)
    
    # Build whale context
    whale_ctx = ""
    cid = detection.get("condition_id", "")
    if cid:
        whales = lookup_whales(cid)
        if whales:
            total_v = sum(w.get("usd_value", 0) or 0 for w in whales)
            high = [w for w in whales if (w.get("trust_score") or 0) >= 6]
            ctx_lines = [f"Total whale volume: ${total_v:,.0f}"]
            classes: dict[str, int] = {}
            for w in whales:
                cls = w.get("classification", "unknown") or "unknown"
                classes[cls] = classes.get(cls, 0) + 1
            ctx_lines.append("Breakdown: " + ", ".join(f"{k}={v}" for k, v in sorted(classes.items())))
            if high:
                ctx_lines.append("Top: " + "; ".join(
                    f"{w['whale_name']}(trust={w['trust_score']}, ${w.get('usd_value', 0):,.0f}, {w.get('side', '?')} {w.get('outcome', '?')})"
                    for w in high[:3]
                ))
            whale_ctx = "\n".join(ctx_lines)
    
    # Build midpoint context (NEW: inject live prices into prompt)
    midpoint_lines = ""
    if midpoints:
        mp_entries = "\n".join(f"  {k}: ${v:.4f}" for k, v in midpoints.items())
        midpoint_lines = f"\nLIVE CLOB MIDPOINTS:\n{mp_entries}"
    
    # Build market metadata context (NEW: resolution date, category)
    meta_ctx = ""
    if market_info:
        end_date = market_info.get("end_date_iso") or market_info.get("endDate", "")
        category = market_info.get("category", "")
        volume = market_info.get("volume", 0) or 0
        meta_ctx = f"""
MARKET METADATA:
  Category: {category or 'unknown'}
  Resolution: {end_date or 'unknown'}
  24h Volume: ${volume:,.0f}"""
    
    # Time-awareness note
    time_note = ""
    if age > 1800:
        time_note = "\n⚠️ Signal is over 30 min old. Verify price before entering."
    elif age > 300:
        time_note = "\n⚡ Signal is 5+ min old. Check midpoint freshness."
    
    prompt = f"""Analyze this Polymarket market and output a trade recommendation as JSON only.

MARKET: {market}
Detection price: ${price:.2f}
Signal age: {age:.0f}s{time_note}
Trades in last scan: {detection.get('trades_count', 0)}
Detection type: {detection.get('type', 'unknown')}{midpoint_lines}{meta_ctx}
{f"Whale Activity:\n{whale_ctx}" if whale_ctx else ""}

EVALUATION:
1. Price edge: Is detection price below current midpoint?
2. Whale quality: High-trust whales aligned?
3. Time horizon: Enough time before resolution?
4. Category: Reliable resolution history?

DECISION RULES:
- BUY: confidence > 0.65, clear price edge, whale support
- WAIT: uncertain, low liquidity, resolution < 6h
- SKIP: noise, conflicting signals, no edge

OUTPUT (JSON only):
{{
  "market": "name",
  "decision": "BUY | WAIT | SKIP",
  "confidence": 0.0-1.0,
  "reason": "brief reason citing specific factors",
  "entry_price": 0.0,
  "target_price": 0.0,
  "stop_price": 0.0,
  "kelly_fraction": 0.0-0.125,
  "hold_hours": 0
}}"""
    
    llm_out = query_llm(prompt)
    
    # Clean thinking markers from Qwen models
    cleaned = llm_out
    cleaned = re.sub(r'<think>.*?</think>', '', cleaned, flags=re.DOTALL)
    cleaned = re.sub(r'<tool_call>', '', cleaned)
    cleaned = re.sub(r'</tool_call>', '', cleaned)
    
    # Strip known thinking prefixes
    for prefix in [
        "Here's a thinking process:",
        "Thinking Process:",
        "Let me think about this:",
        "I'll analyze",
        "Okay, let me",
    ]:
        if prefix in cleaned:
            cleaned = cleaned.split(prefix, 1)[-1]
    
    # Extract the LAST valid JSON object (handles thinking preamble and drafts)
    last_valid: Optional[dict] = None
    stack: list[int] = []
    for i, ch in enumerate(cleaned):
        if ch == '{':
            stack.append(i)
        elif ch == '}' and stack:
            start = stack.pop()
            if not stack:  # top-level object closed
                try:
                    last_valid = json.loads(cleaned[start:i + 1])
                except json.JSONDecodeError:
                    pass
    if last_valid:
        # Clamp Kelly fraction to bounds (NEW: safety gate)
        if "kelly_fraction" in last_valid:
            last_valid["kelly_fraction"] = max(KELLY_MIN, min(KELLY_MAX, last_valid["kelly_fraction"]))
        return last_valid

    # Fallback: try partial parse from truncated output
    if '"decision"' in cleaned and '"confidence"' in cleaned:
        decision_match = re.search(r'"decision":\s*"([^"]+)"', cleaned)
        conf_match = re.search(r'"confidence":\s*([0-9.]+)', cleaned)
        if decision_match and conf_match:
            return {
                "market": market,
                "decision": decision_match.group(1),
                "confidence": float(conf_match.group(1)),
                "reason": f"Partial parse (truncated): {cleaned[:80]}",
                "entry_price": price,
                "target_price": min(price * 1.5, 0.95),
                "stop_price": max(price * 0.9, 0.05),
                "kelly_fraction": max(KELLY_MIN, min(KELLY_MAX, 0.1)),  # Clamped fallback
                "hold_hours": 24,
                "whale_context": whale_ctx if whale_ctx else "",
            }
    
    return {
        "market": market,
        "decision": "SKIP",
        "confidence": 0.0,
        "reason": f"LLM parse failed: {llm_out[:150]}",
        "whale_context": whale_ctx if whale_ctx else "",
    }


def load_detections() -> list[dict]:
    """Load detections from signal monitor output file."""
    if not DETECTIONS_FILE.exists():
        return []
    try:
        data = json.loads(DETECTIONS_FILE.read_text())
        if isinstance(data, list):
            return data
        return [data]
    except json.JSONDecodeError as exc:
        logger.error("Invalid detections file: %s", exc, extra={"path": str(DETECTIONS_FILE)})
        return []


def is_noise(detection: dict) -> bool:
    """Check if detection matches noise patterns (regex-based)."""
    import re
    market = (detection.get("market", "") or "").lower()
    title = (detection.get("title", "") or "").lower()
    combined = market + " " + title
    
    # Regex patterns for known junk markets
    noise_patterns = [
        r"highest\s+temperature",
        r"(bitcoin|ethereum|solana)\s+(up|down)\s+(up|down)",
        r"weather\s+(in|for)",
        r"daily\s+(high|low)\s+temperature",
        r"will\s+\w+\s+score\s+(over|under)",
    ]
    return any(re.search(p, combined, re.I) for p in noise_patterns)


def make_detection_key(detection: dict) -> str:
    """Create a unique key for a detection (includes condition_id for collision safety)."""
    ts = detection.get("detected_at") or detection.get("timestamp", "")
    market = detection.get("market", "") or detection.get("title", "")
    whale = detection.get("whale_name", "")
    cid = detection.get("condition_id", "")  # NEW: prevent collisions
    return f"{cid}:{ts}:{whale}:{market[:50]}"


def run_once(state: BridgeState, timeout_secs: int = RUN_TIMEOUT_SECS) -> list[dict]:
    """Run a single analysis pass. Returns list of new recommendations."""
    logger.info("Starting analysis pass (timeout=%ds)", timeout_secs, extra={"timeout": timeout_secs})
    start_time = time.time()
    
    detections = load_detections()
    logger.info("Loaded %d detections", len(detections), extra={"count": len(detections)})
    
    # Filter out already processed and noise
    new_detections = []
    for det in detections:
        key = make_detection_key(det)
        if state.is_processed(key):
            continue
        if is_noise(det):
            logger.debug("Skipping noise: %s", det.get("market", "?")[:50], extra={"market": det.get("market", "")})
            continue
        new_detections.append(det)
    
    logger.info("%d new detections to analyze", len(new_detections), extra={"count": len(new_detections)})
    
    recommendations = []
    processed_count = 0
    timeout_skipped = 0
    
    for det in new_detections[-5:]:  # Limit to 5 most recent (time is primary limiter)
        elapsed = time.time() - start_time
        remaining = timeout_secs - elapsed
        
        # Check if we have enough time for at least one full cycle (30s minimum)
        if remaining < 30:
            logger.warning(
                "Time budget exceeded, breaking after %d detections (%.1fs elapsed, %.1fs remaining)",
                processed_count, elapsed, remaining,
                extra={"processed": processed_count, "elapsed": elapsed, "remaining": remaining}
            )
            timeout_skipped = len(new_detections[-5:]) - processed_count
            break
        
        market_name = det.get("market", "?")[:50]
        logger.info("Analyzing: %s (%.1fs remaining)", market_name, remaining, extra={"market": market_name})
        
        cid = det.get("condition_id", "")
        market_info = get_market_info(cid) if cid else None
        midpoints = check_midpoint(cid) if cid else {}
        
        rec = analyze_market(det, market_info, midpoints)
        rec["timestamp"] = datetime.now(timezone.utc).isoformat()
        rec["condition_id"] = cid
        rec["detection_price"] = det.get("entry_price") or det.get("lowest_price")
        rec["midpoints"] = midpoints
        
        recommendations.append(rec)
        
        # Mark as processed
        key = make_detection_key(det)
        state.mark_processed(key, rec["timestamp"])
        
        # Log result
        status = "🟢" if rec.get("decision") == "BUY" else ("🟡" if rec.get("decision") == "WAIT" else "⚫")
        whale_note = ""
        whale_ctx = rec.get("whale_context", "")
        if whale_ctx:
            name_match = re.search(r"(\w+)\s+\(trust=", whale_ctx)
            if name_match:
                whale_note = f" | whale: {name_match.group(1)}"
        logger.info(
            "%s %s | %.0f%% | %s",
            status, rec.get("decision", "?"), rec.get("confidence", 0) * 100, rec.get("reason", "")[:60],
            extra={"decision": rec.get("decision"), "confidence": rec.get("confidence"), "whale": whale_note}
        )
        
        # Save incrementally
        save_recommendation(rec)
        processed_count += 1
        time.sleep(1)
    
    total_elapsed = time.time() - start_time
    total_pending = len(new_detections[-5:])
    logger.info(
        "Run timeout: processed %d of %d detections in %.1fs (%d skipped due to time)",
        processed_count, total_pending, total_elapsed, timeout_skipped,
        extra={"processed": processed_count, "total": total_pending, "elapsed": total_elapsed, "skipped": timeout_skipped}
    )
    
    return recommendations


def save_recommendation(rec: dict) -> None:
    """Append a recommendation to the output file and write to Bitable."""
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    try:
        existing = json.loads(OUTPUT_FILE.read_text()) if OUTPUT_FILE.exists() else []
        if not isinstance(existing, list):
            existing = [existing]
    except json.JSONDecodeError:
        existing = []
    
    existing.append(rec)
    # Keep last 100 recommendations
    existing = existing[-100:]
    OUTPUT_FILE.write_text(json.dumps(existing, indent=2))
    
    # Write to Research Log Bitable (only for actionable signals)
    decision = rec.get("decision", "SKIP")
    if decision in ("BUY", "WAIT"):
        try:
            confidence = rec.get("confidence", 0)
            reason = rec.get("reason", "")[:100]
            entry = {
                "文本": f"{rec.get('market', 'Unknown')} | {decision} | {confidence:.0%} | {reason}",
                "单选": decision,
            }
            write_research_log(entry)
        except Exception as e:
            logger.warning("Bitable write failed: %s", e, extra={"error": str(e)})


def run_daemon(interval: int = POLL_INTERVAL_SECS, timeout_secs: int = RUN_TIMEOUT_SECS) -> None:
    """Run as continuous daemon with given interval."""
    logger.info("Starting autoresearch daemon (interval=%ds, timeout=%ds)", interval, timeout_secs, extra={"interval": interval, "timeout": timeout_secs})
    state = BridgeState.load(STATE_FILE)
    try:
        while True:
            state.last_run = datetime.now(timezone.utc).isoformat()
            run_once(state, timeout_secs=timeout_secs)
            state.save(STATE_FILE)
            time.sleep(interval)
    except KeyboardInterrupt:
        logger.info("Daemon stopped by user", extra={})
        state.save(STATE_FILE)


def setup_logging(verbose: bool = False) -> None:
    """Configure logging. DEBUG if verbose, otherwise INFO."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S"
    )


def main() -> None:
    """CLI entry point: --once for cron, default for daemon mode."""
    parser = argparse.ArgumentParser(description="Autoresearch Bridge - Analyze detections and produce trade recommendations")
    parser.add_argument("--once", action="store_true", help="Run once and exit (for cron)")
    parser.add_argument("--interval", type=int, default=POLL_INTERVAL_SECS, help=f"Polling interval (default: {POLL_INTERVAL_SECS})")
    parser.add_argument("--timeout", type=int, default=RUN_TIMEOUT_SECS, help=f"Max seconds for single pass (default: {RUN_TIMEOUT_SECS})")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging")
    args = parser.parse_args()
    
    setup_logging(args.verbose)
    state = BridgeState.load(STATE_FILE)
    
    if args.once:
        logger.info("Running single pass (timeout=%ds)", args.timeout, extra={"timeout": args.timeout})
        recommendations = run_once(state, timeout_secs=args.timeout)
        state.last_run = datetime.now(timezone.utc).isoformat()
        state.save(STATE_FILE)
        buys = sum(1 for r in recommendations if r.get("decision") == "BUY")
        print(f"Analyzed {len(recommendations)} detections → {buys} BUY signals")
        sys.exit(0)
    else:
        run_daemon(interval=args.interval, timeout_secs=args.timeout)


if __name__ == "__main__":
    main()
