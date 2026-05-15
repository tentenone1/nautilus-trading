# Trade Performance Analysis — May 2026

## Overall: +$86,397 PnL across 1,125 closed trades with actual_pnl data

## 🔴 CRITICAL FINDINGS

### Finding 1: Edge Scores Are Inverted
**Evidence:** Edge score buckets vs actual PnL:

| Edge Bucket | Trades | Total PnL | Avg PnL |
|-------------|--------|-----------|---------|
| 0.90+       | 96     | **-$170** | -$1.78  |
| 0.80-0.89   | 609    | **-$5,005** | -$8.22  |
| 0.70-0.79   | 103    | **-$79** | -$0.78  |
| 0.60-0.69   | 91     | **+$28,853** | +$317  |
| 0.50-0.59   | 180    | **+$60,001** | +$333  |
| 0.40-0.49   | 36     | **+$2,798** | +$78   |

**Conclusion:** The edge score system is producing **inverse signals**. High edge = loss, low edge = profit. This is the single largest source of alpha leakage — the system is systematically avoiding the trades that make money and taking the trades that lose money.

### Finding 2: Market Classification Is Severely Broken
**Evidence:** The "general" category contains mostly misclassified sports and crypto markets:
- `Spread: Independiente Santa Fe (-1.5)` → classified as "general" (it's sports)
- `Counter-Strike: Aurora Gaming vs The Huns` → classified as "general" (it's esports)
- `Atlanta Braves vs. Los Angeles Dodgers` → classified as "general" (it's MLB sports)
- `Levante UD vs. CA Osasuna: Draw at halftime?` → classified as "general" (soccer)

The **+$88K** "general" profit is not from a general strategy — it's from sports/crypto trades that happened to be misclassified as general and also happened to win. The classification breakdown means category-based risk controls are ineffective.

### Finding 3: Crypto — 99.2% Win Rate but Negative PnL
**Evidence:**
- 244 wins / 2 losses by exit_price — 99.2% win rate
- But **actual_pnl: -$328** (498 trades)

| Exit Reason | Trades | Total PnL | Avg PnL |
|-------------|--------|-----------|---------|
| resolved    | 361    | +$2,196   | +$6     |
| market_resolved | 131 | **-$1,952** | -$14.91 |
| max_hold    | 2      | **-$595** | -$297   |

**Root cause:** "Resolved" exits capture tiny wins (+$6 avg). When markets go against us, they resolve against us and we take full losses (-$14.91 avg). The system has **no stop-loss mechanism** — it wins small, loses big.

### Finding 4: Sports — The Same Pattern, Worse
**Evidence:**
- 148 wins / 26 losses — 85.1% win rate
- But **actual_pnl: -$2,668** (349 trades)

| Exit Reason | Trades | Total PnL | Avg PnL |
|-------------|--------|-----------|---------|
| market_resolved | 49 | **-$2,559** | -$52.24 |
| resolved    | 278    | -$1,078   | -$3.88  |

**Root cause:** Same as crypto — tiny wins, catastrophic losses. Sports spreads amplify this because spread outcomes are binary and the edge model doesn't account for the 0.5-point margin.

### Finding 5: Position Size Sweet Spot
| Size Bucket | Trades | Total PnL | Avg PnL |
|-------------|--------|-----------|---------|
| $0-10       | 781    | +$1,893   | +$2     |
| $50-100     | 111    | **+$24,816** | +$223   |
| $100-200    | 166    | **+$60,209** | +$362   |
| $200+       | 28     | **-$1,293** | -$46    |

**Conclusion:** $50-200 is the sweet spot. $200+ positions underperform — probably from overconfidence in bad signals. $0-10 is noise.

### Finding 6: Fat-Tail Dependence
The top 5 "general" trades produced **+$43,614** — half the total profit. The system is dependent on a few massive winners rather than consistent edge. This is high-variance, not sustainable.

---

## 🎯 PRIORITIZED FIX PLAN

### P0 — Edge Score Calibration (highest ROI)
**Problem:** Edge score is producing inverse signals — the system systematically avoids profitable trades.
**Fix:** 
1. Research the edge scoring algorithm (wf_edge_calibrator.py)
2. Identify the inversion bug
3. Fix to produce correct directional signals
4. Validate against historical data

### P0 — Market Classification Fix
**Problem:** Sports spreads, crypto binaries, and esports all go to "general" — defeating category-based risk.
**Fix:**
1. Fix wf_signal_router.py / wf_sports.py classification logic
2. Add detection for "Spread:", "O/U", "vs." patterns → sports
3. Add esports keyword detection
4. Ensure all markets land in correct category

### P1 — Stop-Loss / Take-Profit per Category
**Problem:** No exit asymmetry — positions are held until resolution, creating "win small, lose big" pattern.
**Fix:**
1. Add category-based take-profit (e.g., 80% of max expected profit)
2. Add category-based stop-loss (e.g., cut at -50% for crypto, -30% for sports)
3. Override current hold-until-resolution logic for losing positions

### P1 — Position Sizing Cap at $200
**Problem:** $200+ positions underperform across all categories.
**Fix:**
1. Cap max position at $200
2. Review Kelly sizing formula for over-concentration

### P2 — Sports Spread Modeling
**Problem:** Sports spreads need different edge calculation and exit logic than binary markets.
**Fix:**
1. Research sports-specific exit logic
2. Add spread-aware edge calculations

### P2 — SELL Side Trading
**Problem:** Only 37 SELL trades vs 1,088 BUY — missing half the market.
**Fix:**
1. Enable sell-side signal generation
2. Test on low-risk paper markets first
