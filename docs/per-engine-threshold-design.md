# Per-Engine Threshold Design
**Status**: Design Only — Do Not Implement
**Date**: 2026-05-27
**Author**: Claude session

---

## Context

The current unified pipeline applies a single edge scorer (`wf_edge_scorer.py`) across all signal types. This is statistically invalid because the four signal engines produce fundamentally different distributions:

| Engine | Volume | Distribution Shape | Edge Signal Quality |
|---|---|---|---|
| **Sybil** | 1,072 signals (52% of pipeline) | Bimodal (near-0 and near-0.3) | Anomaly detection — whale_wr logic doesn't apply |
| **Sports Whale** | ~1,000 signals | Right-skewed, clustered at 0.12 | Momentum-following, temporal decay critical |
| **Geopolitics** | ~300 signals | Heavy-tailed, fat candles | Event-driven, mean-reversion on resolution |
| **Research/Autoresearch** | 5 signals (historical) | Sparse, high-edge | LLM reasoning, uncorrelated with whale_wr |

Applying a single threshold to these four distributions is statistically invalid and causes two systematic failure modes:
1. **False negatives** — valid signals rejected by wrong threshold
2. **False positives** — noise signals pass a threshold calibrated for a different distribution

---

## Problem 1: The unified threshold is calibrated for none of them

### Sybil (n=1,072, 52% of pipeline)
- **Current behavior**: Edge scorer assigns avg edge_score ≈ 0.05. All 1,225 sybil-in-decision_snapshots die at `edge_below_tier`.
- **Root cause**: The edge scorer uses `whale_wr` as its primary signal (45% weight). Sybil signals have no "whale" — they represent coordinated retail behavior, not individual whale activity. `whale_wr` is structurally inapplicable.
- **Evidence**: Sybil signals in `decision_snapshots` show `whale_name` = `sybil_sybil_group_1/3` etc. The join to compute `whale_wr` fails (0 rows matched in sybil_signals), so the scorer defaults to a near-zero score.
- **Impact**: 1,225 sybil signals blocked on a gate that can't meaningfully evaluate them.

### Sports Whale (n≈1,000 signals)
- **Current behavior**: Rejected by `sports_handler_quarantine` (414 signals), `sports_confidence_below_min` (327), `tier_confidence<25%` (308), `sports_edge_below_min` (56).
- **Root cause**: Sports signals use the same `edge_below_tier` gate with general-market tier thresholds. Sports markets resolve in hours/days with high temporal decay — a 0.30 edge at signal time may decay to 0.05 by execution.
- **Evidence**: 446 signals have `edge_score = 0.30` (the named-whale bypass floor) but are blocked at sports-specific gates, not the edge gate.

### Geopolitics (n≈300)
- **Current behavior**: Falls into `general` bucket. Edge scorer uses `whale_wr` but geopolitical events are event-driven, not history-repeat-driven.
- **Root cause**: Whale historical win rate is a poor predictor for one-off geopolitical events. The edge scorer doesn't account for event novelty or news sentiment.

### Autoresearch (n=5)
- **Current behavior**: Only 5 signals ever generated. In `decision_snapshots` with `edge_ignore` rejections (edge_score=0).
- **Root cause**: The LLM signal generator is producing very few signals. The edge scorer can't evaluate signals it can't see.

---

## Problem 2: Shadow ledger needs entry_price to compute P&L

The `compute_hypothetical_pnl()` function in `wf_shadow_ledger.py` requires `entry_price` and `position_size_usd`. Sports signals blocked at the `sports_telemetry` gate have `position_size_us=0` because sizing is computed AFTER the pipeline check, not before.

Currently, the backfill inserts these with `entry_price=0.0`, which makes P&L computation impossible (`compute_hypothetical_pnl` returns `None` when price=0).

---

## Design: Independent Threshold Profiles

### Option A: Per-Engine Tiering (Recommended)

Add a `SignalEngineClassifier` that routes signals to engine-specific threshold profiles before the unified edge scorer runs:

```
SignalEngineClassifier
├── detect_engine(signal) → sybil | sports | geopolitics | autoresearch
├── route_to_profile(engine, signal) → ThresholdProfile
└── profile.apply(signal) → passed | rejected(reason)

ThresholdProfile(engines):
  sybil:
    edge_min: 0.10        # calibrated for anomaly-detection signals
    confidence_min: 0.40   # need high conviction on group signals
    wr_weight: 0.0        # IGNORE whale_wr — structurally inapplicable
    anomaly_weight: 0.50   # weight group coherence signal
    blacklist_bypass: True # sybil groups can bypass if coherent

  sports:
    edge_min: 0.15        # slightly lower than general (temporal decay)
    confidence_min: 0.35  # sports confidence is noisier
    quarantine_required: True
    temporal_decay_window: 3600  # seconds — penalize stale signals
    wr_weight: 0.25       # whale_wr still matters for sports momentum
    bypass_eligible: ["model_insider", "autoresearch_llm"]

  geopolitics:
    edge_min: 0.20       # higher bar — events are infrequent/high-impact
    confidence_min: 0.50  # need strong LLM confidence
    novelty_weight: 0.40  # weight news/event novelty score
    wr_weight: 0.15      # whale_wr less predictive for one-off events
    blacklist_bypass: False

  autoresearch:
    edge_min: 0.10       # let LLM signals through with lower bar
    confidence_min: 0.60  # but require high confidence
    llm_weight: 0.60     # primary weight on LLM reasoning score
    wr_weight: 0.0       # ignore whale_wr for LLM signals
```

### Option B: Hybrid — Run Both, Take Max

Keep the unified pipeline for safety (regression protection) but compute a per-engine parallel score and use the **maximum** of the two:

```python
def compute_edge_score(signal, pipeline_result):
    unified_score = compute_unified_edge(signal)  # current system

    if signal.source == "sybil":
        engine_score = compute_sybil_score(signal)
    elif is_sports(signal):
        engine_score = compute_sports_score(signal)
    elif is_geopolitics(signal):
        engine_score = compute_geopolitics_score(signal)
    else:
        engine_score = unified_score

    # Use the more permissive score (reduce false negatives)
    return max(unified_score, engine_score)
```

**Risk**: Option B could increase false positives if the engine score is systematically overestimated.

---

## Recommended Next Steps (Design Phase Only)

1. **Collect distribution statistics** for each engine over a 30-day window. Plot edge_score histograms per engine to validate the "different distributions" hypothesis with statistical tests (Kolmogorov-Smirnov).
2. **Define `SignalEngineClassifier`** — a function that inspects `signal.source`, `signal.category`, `signal.market_title` keywords, and `signal.whale_name` prefix to route to the correct engine.
3. **Backtest Option A vs Option B** using the shadow ledger data (which is now populating with real signals). Use the hypothetical P&L to evaluate whether per-engine thresholds would have improved outcomes.
4. **Design the `TemporalDecayCalculator`** for sports signals — given signal timestamp and market end time, compute a decayed edge score.
5. **Do NOT implement** until shadow ledger has ≥50 resolved sports signals with P&L data to calibrate the thresholds against.

---

## Open Questions

1. **Sybil join key**: The `decision_snapshots.signal_id` (UUID) cannot join to `sybil_signals.condition_id`. Until this is fixed, sybil edge scores will always be ~0.05 default.
2. **Shadow ledger entry_price**: Sports signals blocked at `sports_telemetry` need `entry_price` populated. This requires either (a) computing position size before the pipeline gate, or (b) using the Polymarket current mid-price at signal time as a proxy for entry price.
3. **Autoresearch volume**: Only 5 signals ever generated. Before designing thresholds for this engine, investigate why volume is so low.

---

## Files to Modify (When Implemented)

- `strategies/signal_pipeline.py` — add `SignalEngineClassifier` and per-engine routing
- `strategies/wf_edge_scorer.py` — add `compute_sybil_score()`, `compute_sports_score()`, `compute_geopolitics_score()`
- `strategies/wf_constants.py` — add `ENGINE_THRESHOLDS` config block (frozen after backtest validation)
- `strategies/wf_shadow_ledger.py` — handle `entry_price=0` for sports signals using Polymarket mid-price proxy
