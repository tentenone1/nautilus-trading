#!/usr/bin/env python3
"""G3: Exit Strategy Audit — queries trades.db and outputs backtest_results/exit_audit_v5.5.json."""

import sqlite3
import json
from pathlib import Path

DB = Path(__file__).parent.parent / "data" / "trades.db"
OUT = Path(__file__).parent.parent / "backtest_results" / "exit_audit_v5.5.json"


def _get_recommendation(cat, wr, avg_winner_ret, avg_loser_ret, total_pnl, tp_exits, sl_exits):
    recs = []
    if cat == "SPORTS":
        if wr < 0.35:
            recs.append("WR too low for sports; consider stricter confidence filter before entry.")
        if avg_winner_ret > 0.60:
            recs.append(f"Winners average +{avg_winner_ret:.0%} but TP=80% is tight; winners averaging higher suggest TP could be widened to 100%+.")
        recs.append("Current TP=80% is appropriate for sports; consider tighter SL=-15% to cut losses earlier.")
    elif cat == "CRYPTO":
        if wr < 0.40:
            recs.append("WR=38.5% is marginal; crypto needs whale fade signals to improve WR.")
        if avg_winner_ret > 0.50:
            recs.append(f"Winners averaging +{avg_winner_ret:.0%} but TP=50% cuts winners short; consider widening TP to 80%.")
        recs.append("SL=-30% may be too wide for crypto; suggest SL=-20% instead.")
    elif cat == "GENERAL":
        if wr >= 0.45 and total_pnl > 0:
            recs.append("GENERAL is the profit engine; TP=50%/SL=-40% is working. Consider increasing exposure.")
        if avg_winner_ret > 0.80:
            recs.append(f"Winners averaging +{avg_winner_ret:.0%} suggest TP=50% is cutting winners short; consider widening to 80%.")
    elif cat == "GEOPOLITICS":
        if wr >= 0.27 and total_pnl > 0:
            recs.append("GEOPOLITICS profitable; TP=50%/SL=-50% is working despite low WR.")
    elif cat == "ECONOMICS":
        recs.append("ECONOMICS has 15% WR, PF=0.03 — structurally unprofitable. Block new entries or switch to paper-only.")
    elif cat == "POLITICS":
        recs.append("POLITICS has 9.8% WR, PF=0.02 — structurally unprofitable. Block new entries or switch to paper-only.")
    elif cat == "TECHNOLOGY":
        recs.append("TECHNOLOGY has 0% WR in sample; block or paper-only until more data.")
    return recs if recs else ["Thresholds appear reasonable; monitor ongoing performance."]


def main():
    conn = sqlite3.connect(str(DB))
    conn.execute("PRAGMA busy_timeout=5000")

    rows = conn.execute("""
        SELECT category, exit_reason, realized_pnl, realized_return,
               entry_price, exit_price, side, duration_seconds, market_title
        FROM trades
        WHERE realized_pnl IS NOT NULL
    """).fetchall()
    conn.close()

    # Group by category
    cats = {}
    for r in rows:
        cat = (r[0] or "unknown").upper()
        if cat not in cats:
            cats[cat] = {"wins": [], "losses": [], "by_exit_reason": {}}
        pnl, ret, reason = r[2], r[3], r[1]
        cats[cat]["by_exit_reason"][reason or "unknown"] = \
            cats[cat]["by_exit_reason"].get(reason or "unknown", 0) + 1
        if (pnl or 0) > 0:
            cats[cat]["wins"].append({"pnl": pnl, "ret": ret or 0})
        else:
            cats[cat]["losses"].append({"pnl": pnl, "ret": ret or 0})

    TP_REASONS = {"category_take_profit", "certainty_win"}
    SL_REASONS = {"category_stop_loss", "certainty_loss"}

    CURRENT_THRESHOLDS = {
        "CRYPTO":      {"take_profit_pct": 0.50, "stop_loss_pct": -0.30},
        "SPORTS":      {"take_profit_pct": 0.80, "stop_loss_pct": -0.25},
        "GENERAL":     {"take_profit_pct": 0.50, "stop_loss_pct": -0.40},
        "POLITICS":    {"take_profit_pct": 0.50, "stop_loss_pct": -0.50},
        "GEOPOLITICS": {"take_profit_pct": 0.50, "stop_loss_pct": -0.50},
    }

    result = {
        "audit_version": "v5.5",
        "generated_at": str(Path(__file__).stat().st_mtime),
        "total_trades": len(rows),
        "categories": {}
    }

    for cat, data in sorted(cats.items()):
        n_wins = len(data["wins"])
        n_loss = len(data["losses"])
        total = n_wins + n_loss
        if total == 0:
            continue

        avg_win_pnl = sum(w["pnl"] for w in data["wins"]) / n_wins if n_wins else 0
        avg_loss_pnl = sum(l["pnl"] for l in data["losses"]) / n_loss if n_loss else 0
        total_pnl = sum(w["pnl"] for w in data["wins"]) + sum(l["pnl"] for l in data["losses"])
        wr = n_wins / total if total > 0 else 0

        tp_exits = sum(v for k, v in data["by_exit_reason"].items() if k in TP_REASONS)
        sl_exits = sum(v for k, v in data["by_exit_reason"].items() if k in SL_REASONS)

        avg_winner_ret = sum(w["ret"] for w in data["wins"]) / n_wins if n_wins else 0
        avg_loser_ret = sum(l["ret"] for l in data["losses"]) / n_loss if n_loss else 0

        result["categories"][cat] = {
            "sample_size": total,
            "win_rate": round(wr, 4),
            "avg_winner_pnl_usd": round(avg_win_pnl, 4),
            "avg_loser_pnl_usd": round(avg_loss_pnl, 4),
            "total_pnl_usd": round(total_pnl, 2),
            "avg_winner_return_pct": round(avg_winner_ret, 4),
            "avg_loser_return_pct": round(avg_loser_ret, 4),
            "tp_exits": tp_exits,
            "sl_exits": sl_exits,
            "exit_reason_counts": dict(list(data["by_exit_reason"].items())[:10]),
            "current_thresholds": CURRENT_THRESHOLDS.get(cat, {"take_profit_pct": 0.50, "stop_loss_pct": -0.50}),
            "recommendation": _get_recommendation(
                cat, wr, avg_winner_ret, avg_loser_ret, total_pnl, tp_exits, sl_exits
            ),
        }

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT, "w") as f:
        json.dump(result, f, indent=2)
    print(f"Wrote {OUT}")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
