# Jailbreak Whale Analysis Output

**Generated:** 2026-05-19 16:06 UTC
**Source:** `scripts/jailbreak_analysis.py`
**DB:** 149 named whales, 63 analyzed → 10 signals

---

## Signals Summary

| Whale | Action | Confidence | Reason |
|-------|--------|------------|--------|
| autoresearch_llm | COPY | 0.85 | High volume (644 trades), positive PnL, 48% win rate — consistent edge |
| surf | COPY | 0.75 | Small sample (5 trades), high PnL, 33% win rate — profitable trend |
| JewishNinja | COPY | 0.70 | 39 trades, positive PnL, 21% win rate — high payout efficiency |
| p167-0x033f03 | COPY | 0.80 | 50% win rate, positive PnL, 18 trades — reliable performance |
| p32-0xe72bb5 | COPY | 0.65 | 0% win rate but positive PnL — successful high-risk/high-reward betting |
| p37-0xe5efd6 | FADE | 0.70 | 30% win rate, negative PnL — poor performance despite volume |
| p232-0xd10695 | FADE | 0.90 | 0% win rate, negative PnL across 12 trades — consistent losing streak |
| p102-0xf68a28 | FADE | 0.75 | 25% win rate, negative PnL, 19 trades — poor strategy |
| p137-0x681504 | FADE | 0.65 | 46% win rate but negative PnL — insufficient payout efficiency |
| p183-0x437961 | FADE | 0.60 | 42% win rate but negative PnL — underperforming strategy |

---

## COPY Signals (5)

1. **autoresearch_llm** — High conviction. 644 trades, 48% WR, positive PnL. The strongest signal.
2. **p167-0x033f03** — Reliable. 50% WR, positive PnL, 18 trades.
3. **surf** — Trend play. Small but profitable.
4. **JewishNinja** — Payout efficiency king. 21% WR but positive PnL across 39 trades.
5. **p32-0xe72bb5** — High-risk/high-reward. 0% WR but positive PnL — likely hitting long-shot payouts.

## FADE Signals (5)

1. **p232-0xd10695** — Extreme caution. 0% WR across 12 trades with negative PnL.
2. **p102-0xf68a28** — Negative edge. 25% WR, negative PnL across 19 trades.
3. **p37-0xe5efd6** — YOLO gambler. 30% WR, negative PnL, $230 avg bet.
4. **p137-0x681504** — Payout inefficiency. 46% WR sounds good but negative PnL.
5. **p183-0x437961** — Underperforming. 42% WR sounds decent but negative PnL.

---

## Key Insight

The FADE whales with positive win rates (p137 at 46%, p183 at 42%) are being fooled by low-probability/long-shot markets where they win often but lose big. The COPY whales with low win rates (JewishNinja at 21%, p32 at 0%) are succeeding by hitting high-payout bets. **Win rate is not the edge — PnL is.**
