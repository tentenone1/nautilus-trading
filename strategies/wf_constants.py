"""Whale Follower Strategy — Constants and Configuration.

All module-level constants and the WhaleFollowerConfig dataclass,
extracted from whale_follower.py for centralized management.

============================================
v5.5 PHASE D FREEZE — 2026-05-26
============================================
CHANGELOG:
  • G1:  Correlation Gate — block simultaneous copy+fade of correlated whales
  • G2:  Realistic Slippage — 200bps, 70% fill prob (was 0bps)
  • G3:  Exit Strategy Audit — max_hold exit added; pre_res 48h stop-loss confirmed
  • G4:  LLM Degraded-Mode Fallback — signal_bridge.py graceful degradation
  • G5:  Heartbeat Alert Cron — paper trader health monitoring
  • G6:  Edge Scorer Feature Upgrade — category + whale WR + action multiplier
  • C4:  Poly_data Fade Target Backtest — p102-0xf68a28 validated (PARTIAL_OR_HOLD)
  • C5:  deep_value & panic_fade offline — both NEGATIVE, do not deploy

CONFIG:
  • ACTIVE_CONFIG_VERSION = "v5.5"
  • COPY_WIN_RATE_BOOST = 2.0 (edge_scorer.py)
  • FADE_WIN_RATE_BOOST  = 2.2 (edge_scorer.py)

FADE CANDIDATES (paper-only, requires further validation before live):
  • p102-0xf68a28 — MOMENTUM, 26% WR, +$139 live PnL. Fading: +$327 (C4 validated)
    Status: HOLD — do not activate in live until 30-day paper confirms

BLACKLIST ADDITIONS THIS FREEZE:
  • p232-0xd10695 — SKIP (0% WR contrarian, too risky)
  • p37-0xe5efd6  — SKIP (INFORMATION type, wrong for fade)
  • TTEST2         — confirmed in WHALE_BLACKLIST already

STABILITY:
  • Service running since 2026-05-26 12:25:58 CST
  • All Phase A-C tasks complete
  • Phase D freeze complete
============================================
"""

from __future__ import annotations

from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.identifiers import InstrumentId

# ── Configuration Version ──────────────────────────────────────────────────

# Legacy alias — kept so any old code that imports CONFIG_VERSION still works.
CONFIG_VERSION = "v5.4-sports-quarantine-fix"

# Single source of truth for all new trade records. Bump this to v5.6, v5.7,
# etc. whenever a config-changing code change is deployed. The 48h P&L gate
# uses this to detect a code/DB version mismatch and blocks trading if the
# most recent closed trade was recorded under a different version.
ACTIVE_CONFIG_VERSION = "v5.5"


# ── Trade Buffer Thresholds ──────────────────────────────────────────────────

TRADE_BUFFER_SIZE_THRESHOLD = 200  # Minimum USD to buffer a trade
TRADE_BUFFER_FLUSH_COUNT = 5  # Number of trades to trigger buffer flush



# ── Exit Timer Configuration ─────────────────────────────────────────────────

EXIT_TIMER_INTERVAL_SECS = 30.0  # How often to check all positions for exits
RECYCLE_INTERVAL_SECS = 1800.0  # Unsubscribe/resubscribe interval to flush stale order books


# ── Position Management ──────────────────────────────────────────────────────

RE_ENTRY_COOLDOWN_SECS = 300  # Don't re-enter same instrument within 5 minutes of exit
LOW_CASH_ALERT_PCT = 0.20  # Warn when free balance drops below 20% of bankroll


# ── Whale Blacklists (auto-reject proven losers, data from trades.db) ────────

WHALE_BLACKLIST = frozenset({
    # ── Proven losers (overall WR < 20%, should_fade=1) ──
    "TTEST2",           # -17,419 actual P&L, 0% WR
    "weflyhigh",        # 7% WR, should_fade=1
    # ── Entity cluster fades ──
    "AppleTime67",      # entity cluster — should_fade=True (degenerate_human)
    "Dvitaminbets",     # entity cluster — should_fade=True (degenerate_human)
    "Herdonia",         # entity cluster — should_fade=True (degenerate_human)
    "NewTeamSosed4",    # entity cluster — should_fade=True (degenerate_human)
    "Pajamapants",      # entity cluster — should_fade=True (degenerate_human)
    "Talvez10",         # entity cluster — should_fade=True (degenerate_human)
    "beetlepimp",       # entity cluster — should_fade=True (degenerate_human)
    "loitterer",        # entity cluster — should_fade=True (market_maker)
    "pilotlady",        # entity cluster — should_fade=True (degenerate_human)
    "trade-via-Gravia", # entity cluster — should_fade=True (market_maker)
    "Hehaj648jeh",       # entity cluster — mixed_entity
    "phonesculptor",    # entity cluster — mixed_entity
    "sybil_group_1",    # entity cluster — degenerate_human
    "JewishNinja",        # 15% WR proven loser — fade candidate
    "Wannac",           # -1,119 actual P&L, 1 trade only — watch
    "p37-0xe5efd6",     # 0% WR crypto (8 trades, -$1,761) — fade candidate
})

SPORTS_WHALE_BLACKLIST = frozenset({
    # v5.0-emergency-fix: added COMEONDUDE (0% WR in sports, 10 trades, -$43.28)
    "COMEONDUDE",
    "LaBradfordSmith22", # -2,111 on sports (profitable on general)
    "TheVeryGoodCow",    # -613 on sports
    "beetlepimp",        # -399 on sports
    # Profitable overall but lose on sports — fade in sports only
    "asdfjh",           # 84% WR overall, -7,375 on sports
    "bossoskil1",       # 95% WR overall, -6,209 on sports
    "Sassy-Bucket",     # 95% WR overall, -1,277 on sports
    "benwyatt",         # -1,866 on sports
    "JPMorgan101",      # -1,510 on sports
    "joblessfinalboss", # -1,446 on sports
})


# ── Certainty Exit Thresholds (binary prediction markets) ────────────────────

CERTAINTY_WIN_THRESHOLD = 0.95  # Price above this = very likely to win
CERTAINTY_LOSS_THRESHOLD = 0.05  # Price below this = very likely to lose


# ── P&L Sanity Cap ───────────────────────────────────────────────────────────

MAX_SANE_RETURN = 2.0  # Cap P&L returns at +/-200% to prevent sandbox artifacts


# ── Memory Management ────────────────────────────────────────────────────────

MEMORY_PRESSURE_MB = 2500  # RSS threshold in MB to trigger graceful shutdown


# ── Subscription Cleanup ─────────────────────────────────────────────────────

STALE_SUBSCRIPTION_TTL_SECS = 3600  # Clean up dynamic subscriptions older than 1 hour


# ── Resolution Timing ────────────────────────────────────────────────────────

RESOLUTION_EXIT_HOURS = 6  # Exit if market resolves within this many hours
PRE_RESOLUTION_EXIT_HOURS = 48  # DeepSeek P1: exit 48h before if return < -20%
PRE_RESOLUTION_STOP_LOSS_PCT = -0.20  # Exit if position is down more than this %


# ── Sports Market Timing ─────────────────────────────────────────────────────

SPORTS_EXIT_HOURS_BEFORE_EVENT = 3  # Exit sports positions 3 hours before game (was 1)
SPORTS_KELLY_MULTIPLIER = 0.5  # Halved Kelly for sports (38.6% WR vs 55% breakeven)

# SPORTS WHITELIST: Allow sports markets matching these patterns
# Also allows major sports categories (NBA, NFL, UFC, soccer, tennis) regardless of title pattern
# Track record: Spread/handicap bets and major league match winners are profitable
SPORTS_WHITELIST_PATTERNS = [
    r"^Spread:",              # "Spread: Lakers (-3.5)"
    r"^Spread\s*:",           # tolerate "Spread : Lakers"
    r"^Handicap:",            # "Handicap: Lakers -3.5"
    r"^Point\s*Spread:",      # "Point Spread: Team (-3.5)"
    r"^Game\s*Line:",         # "Game Line: Team -3"
    r"\s+-\s+\d+\.?\d*",     # "Lakers - 3.5" (spread with spaces)
    r"\s-\s*\d+\.?\d*",      # "Lakers -3.5", "Team -115" (spread, no space before digit)
    r"\(\s*-?\d+\.?\d+\s*\)$",  # "(+3.5)", "(-3.5)" (odds-style spread notation)
    # Major league match winner markets (high signal quality)
    r"(?i)\b(NBA|NFL|MLB|NHL|UFC|NASCAR|Formula\s*1|F1|UEFA|Premier\s*League|World\s*Cup|Champions\s*League|Super\s*Bowl)\b",
    # Generic team vs team (common Polymarket format, high whale activity)
    # FIXED: use (?<!\w)(?!\w) instead of \b to prevent matching "v" inside words
    r"(?i)(?<!\w)(?:vs|v\.?|@)(?!\w)",
]

# Over/Under market patterns to reject (unprofitable)
SPORTS_OU_BLACKLIST_PATTERNS = [
    r"\bO\s*/\s*U\b",      # "O/U 215.5"
    r"\bOver\s*/\s*Under\b",  # "Over/Under 215.5"
    r"\bOver\b.*\bUnder\b",  # "Over 215.5 / Under"
    r"^Over\s+",           # "Over 215.5"
    r"^Under\s+",          # "Under 215.5"
]

# Head-to-head vs market patterns to reject (unprofitable)
SPORTS_VS_BLACKLIST_PATTERNS = [
    r"\bvs\.?\b",          # "Lakers vs Celtics", "Team A vs Team B"
    r"\bv\.?\b",           # shorthand "v"
    r"\s+-\s+",            # "Lakers - Celtics" (hyphen separator)
]

# Single-team winner market patterns (reject these — 0% WR, >0K losses)
SPORTS_SINGLE_TEAM_PATTERNS = [
    r'^Will\s+.+?\s+win\s+on\s+\d{4}-\d{2}-\d{2}',
    r'^Will\s+.+?\s+win\s+(the\s+)?(next\s+)?(match|game|race|fight)',
    r'^Who\s+will\s+win\s+.+',
    r'^.+\s+to\s+win\s+',
    r'^Winner\s+of\s+',
]

SPORTS_DAILY_LOSS_LIMIT = 5000  # Max daily loss on sports before blocking new positions (raised from $2,000)
SPORTS_AUTO_EXIT_LOSS = 250  # Auto-exit sports positions at -$250 unrealized P&L

# Single-team winner market patterns to reject
# FIXED: was using broad string containment (" win the ") which incorrectly
# blocked legitimate binary prediction markets (e.g. "Will X win the Y?").
# Now uses precise regex anchored to start of string.
SINGLE_TEAM_PATTERNS = [
    r"^Will\s+.+?\s+win\s+on\s+\d{4}-\d{2}-\d{2}",   # "Will X win on 2026-05-15" (date-specific prop)
    r"^Will\s+.+?\s+win\s+the\s+(next|upcoming|this)\s+",   # "Will X win the next match"
    r"^Who\s+will\s+win\s+",                        # "Who will win X"
    r"^Winner\s+of\s+",                             # "Winner of X"
    r"^Make\s+the\s+",                               # "Make the final"
]


# ── Entry Price Filter ─────────────────────────────────────────────────────────
MIN_ENTRY_PRICE = 0.05  # Reject markets priced below $0.05 (filters 166:1 long shots)

# ── Confidence Threshold ───────────────────────────────────────────────────────
MIN_CONFIDENCE = 0.55  # Minimum confidence to enter a trade

# Sports-specific entry filters (stricter for sports markets)
SPORTS_MIN_EDGE = 0.20  # Minimum edge_score for sports signals
SPORTS_MIN_CONFIDENCE = 0.65  # Minimum confidence for sports signals

# ── Sports Quarantine Fade Bypass (v5.5 Phase D) ──────────────────────────
# Whales on this list can be FADED during the sports quarantine.
# Position cap is enforced separately by PositionManager.
# NOTE: whitelist says HOLD until 30-day paper confirms (day 12/30 as of 2026-05-26).
# Adding to the list now so the bypass is ready to activate when confirmation arrives.
FADE_QUARANTINE_BYPASS: frozenset[str] = frozenset({
    # p102-0xf68a28 — MOMENTUM whale, 26% WR in sports. Fading: +$327 (C4 validated).
    # Paper tracking active. DO NOT activate until 30-day paper confirms + audit review.
    "0xf68a281980f8c13828e84e147e3822381d6e5b1b",
})

# ── Liquidity Tier Thresholds (volume + liquidity in USD) ────────────────────

LIQUIDITY_TIER4_THRESHOLD = 100_000  # Illiquid: reduce to 25% of Kelly
LIQUIDITY_TIER3_THRESHOLD = 1_000_000  # Moderate: reduce to 50% of Kelly


# ── Liquidity Sizing Multipliers ─────────────────────────────────────────────

LIQUIDITY_TIER4_MULTIPLIER = 0.25
LIQUIDITY_TIER3_MULTIPLIER = 0.50
LIQUIDITY_TIER2_MULTIPLIER = 0.75


# ── Correlation Gate ───────────────────────────────────────────────────────────

MAX_CORRELATED_POSITIONS = 3   # Reject if ≥3 open positions share a keyword cluster


# ── Phase 1 Risk Control Limits ───────────────────────────────────────────────

# Maximum single position size as % of capital (per position cap)
MAX_SINGLE_POSITION_PCT = 0.02  # 2% of capital

# Maximum total exposure as % of capital (total portfolio exposure)
MAX_TOTAL_EXPOSURE_PCT = 0.20  # 20% of capital

# Maximum per-market exposure as % of capital (per token_id exposure)
MAX_MARKET_EXPOSURE_PCT = 0.05  # 5% of capital

# Fixed capital base for validation mode (Phase 1 testing)
VALIDATION_CAPITAL_BASE = 1000.0  # $1000 for validation


# ── Phase 2 Validation Mode — Whitelist Filters ──────────────────────────────────

# $100 capital validation mode — treat with $100k safety standards
VALIDATION_CAPITAL = 100.0  # Phase 2 validation bankroll
VALIDATION_DAILY_LOSS_LIMIT = 10.0  # 10% daily loss cap for $100 mode
VALIDATION_MAX_POSITION_USD = 2.0  # $2 max single position (2% of $100)
VALIDATION_MAX_CONCURRENT = 5  # Max 5 concurrent positions
VALIDATION_KELLY_FRACTION = 0.10  # 10% Kelly (conservative for validation)

# Routing tiers — entry price caps for live trading (above cap = paper trade)
# None = no cap (live eligible up to any price)
LIVE_ENTRY_PRICE_CAPS: dict[str, float | None] = {
    # Tier 1: fully live — all entries are profitable
    "general":      None,   # All entries profitable, no cap needed
    "geopolitics":  None,   # All 45 trades profitable (+$2,856)
    # Tier 2: paper-only — structural losses despite some whale winners
    "politics":     0.00,   # 9 trades, -$581 avg -$64 — 3 losses offset 2 wins
    "economics":    0.00,   # 10 trades, -$408 avg -$41 — consistently losing
    "technology":   0.00,   # 6 trades, -$625 avg -$104 — sample too small
    # Tier 3: price-gated — profitable below threshold, losers above
    "sports":       0.10,  # $0.05-0.10: +$406 avg | $0.35-0.50: -$9.68 avg
}

# Blocked whale addresses — these whales consistently lose money, never follow them live
# They can still be paper-tracked for data collection
BLOCKED_WHALE_ADDRESSES = frozenset({
    "Hehaj648jeh",                          # 156 crypto trades, -$197 total, avg -$1.26
    "0x14026373da58fabe45e2bf73a915a5d4b3a6e35b",  # 144 crypto trades, -$197 total, avg -$1.36
})

# Categories eligible for live trading (any category in LIVE_ENTRY_PRICE_CAPS)
ALLOWED_CATEGORIES = frozenset({k for k, v in LIVE_ENTRY_PRICE_CAPS.items() if v != 0.0})

# Permanently blocked categories
BLOCKED_CATEGORIES = frozenset({
    "entertainment", # No edge data, unpredictable outcomes
    "finance",       # Low signal quality, no live track record
    "unknown",       # Unclassified markets
    "economics",     # PF 0.03, -$388 from 9 trades — no parameter saves this
})

# Whale type whitelist — only follow proven whale classifications
ALLOWED_WHALE_TYPES = frozenset({
    "skilled_human",       # Consistent profitable traders
    "sacrificial_account", # Entity cluster sacrificial accounts
    "degenerate_human",    # High-volume risk-takers (some profitable)
    "mixed_entity",        # Entity clusters with mixed signals (opened v4.0 for data gathering)
    "unknown",             # Unclassified whales (opened v4.0 for data gathering)
    "whale",               # Generic classification (opened v4.0 for data gathering)
})

# Blocked whale types — reject unverified/unprofitable whale types
BLOCKED_WHALE_TYPES = frozenset({
    "bob",             # Deprecated classification only
})


# ── Sports Market Keywords ───────────────────────────────────────────────────

SPORTS_KEYWORDS: list[str] = [
    "nfl", "nba", "mlb", "nhl", "ncaa", "college football", "college basketball",
    "soccer", "football", "basketball", "baseball", "hockey", "tennis", "golf",
    "boxing", "mma", "ufc", "wwe", "f1", "formula 1", "nascar",
    "super bowl", "world cup", "champions league", "premier league",
    "playoffs", "stanley cup", "world series", "final four", "march madness",
    "vs.", " vs ", "eagles", "49ers", "chiefs", "lakers", "celtics",
    "warriors", "yankees", "dodgers", "red sox", "patriots",
    "trail blazers", "spurs", "penguins", "stars", "wild",
    "bucks", "thunder", "nuggets", "timberwolves", "knicks",
]


class WhaleFollowerConfig(StrategyConfig, frozen=True):
    """Configuration for WhaleFollower."""

    instrument_ids: list[InstrumentId]
    bankroll: float = 10000.0
    kelly_fraction: float = 0.25
    stop_loss_pct: float = 0.15
    take_profit_pct: float = 0.30
    max_position_pct: float = 0.10
    max_open_positions: int = 50
    # Phase 1 risk control limits
    # Max single position size as % of capital (per position cap - 2%)
    max_single_position_pct: float = 0.02
    # Max total exposure as % of capital (total portfolio exposure - 20%)
    max_total_exposure_pct: float = 0.20
    # Max per-market exposure as % of capital (per token_id exposure - 5%)
    max_market_exposure_pct: float = 0.05
    # Fixed capital base for validation mode
    validation_capital_base: float = 1000.0
    # Daily loss limit: stop trading if daily loss exceeds this
    daily_loss_limit: float = 500.0
    sports_daily_loss_limit: float = 2000.0
    min_confidence: float = 0.55
    scan_interval_secs: float = 30.0
    auto_trade: bool = True
    # Dynamic Kelly: use whale's actual win rate instead of fixed estimate
    use_dynamic_kelly: bool = True
    # Seen position TTL: re-scan positions older than this (seconds)
    seen_position_ttl: float = 14400.0  # 4 hours (was 24h - 542 orphan_cleanup_sandbox trades avg'd 35h)
    # Max hold time for open positions (hours) - longer than this triggers auto-exit
    max_hold_hours: float = 4.0  # close positions held > 4h (was 24h - 6.2% WR on >1h positions)

    # Asymmetrical SL/TP: TP = TP_MULTIPLIER x SL threshold (winners run longer)
    tp_multiplier: float = 2.5  # TP width = 2.5x SL width

    # Trailing stop - activates after TP threshold is reached
    trailing_stop: bool = True
    trailing_stop_retrace_pct: float = 0.40  # Exit if price retraces 40% from peak gain
    # Max trades per scan cycle (prevents balance exhaustion on restart)
    max_trades_per_scan: int = 5
    # Trade buffer flush interval (seconds)
    trade_buffer_flush_secs: float = 30.0
    # Test mode: inject synthetic signals to exercise pipeline
    test_mode: bool = False
    test_signal_interval_secs: float = 300.0  # 5 min between synthetic signals

    # Backward compat: allow single instrument_id
    @property
    def instrument_id(self) -> InstrumentId | None:
        return self.instrument_ids[0] if self.instrument_ids else None


def categorize_instrument(inst_id: str) -> str:
    """Fallback categorizer from instrument ID when signal lacks market_title."""
    if not inst_id:
        return "general"
    parts = inst_id.split("-")
    if len(parts) > 1:
        raw = parts[1].replace(".POLYMARKET", "").replace("_", " ").replace("-", " ")
        # Skip numeric-only strings (condition IDs) -- not categorizable
        if raw and raw[0].isdigit() and raw.replace(".", "").replace("_", "").isalnum():
            return "general"
        from strategies.whale_tracker_new import _categorize_market
        result = _categorize_market(raw)
        return result if result != "general" or raw else "general"
    return "general"
