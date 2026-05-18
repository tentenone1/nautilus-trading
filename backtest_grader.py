#!/usr/bin/env python3
"""Backtesting Grader - reads from trades.db for real performance.
Usage: python backtest_grader.py [--days 7] [--json out.json]
"""
import argparse
import json
import math
import sqlite3
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

BASE = Path.home() / "workspace" / "nautilus-trading"
DB = BASE / "research" / "trades.db"
OUT = BASE / "backtest_results"


@dataclass
class T:
    entry: str = ""
    side: str = "buy"
    ep: float = 0.5
    xp: float = 0.5
    sz: float = 0.0
    pnl: float = 0.0
    cat: str = ""
    whale: str = ""
    reason: str = ""


def fetch(days=0):
    if not DB.exists():
        return []
    c = sqlite3.connect(str(DB))
    c.row_factory = sqlite3.Row
    q = "SELECT timestamp,exit_reason,actual_pnl,side,entry_price,exit_price,position_size_usd,category,whale_name FROM trades WHERE exit_reason IS NOT NULL AND actual_pnl IS NOT NULL"
    p = []
    if days > 0:
        q += " AND datetime(timestamp) >= datetime('now', ?)"
        p.append(f"-{days} days")
    q += " ORDER BY timestamp DESC"
    rows = c.execute(q, p).fetchall()
    c.close()
    result = []
    for x in rows:
        result.append(T(
            entry=x["timestamp"] or "", side=x["side"] or "buy",
            ep=x["entry_price"] or 0.5, xp=x["exit_price"] or 0.5,
            sz=x["position_size_usd"] or 0.0, pnl=x["actual_pnl"] or 0.0,
            cat=x["category"] or "unknown", whale=x["whale_name"] or "unknown",
            reason=x["exit_reason"] or "unknown",
        ))
    return result


@dataclass
class R:
    n: int = 0; win: int = 0; los: int = 0; pnl: float = 0.0
    avg_w: float = 0.0; avg_l: float = 0.0; wr: float = 0.0; pf: float = 0.0
    sharpe: float = 0.0; dd: float = 0.0; ret: float = 0.0; g: str = "F"; gr: str = ""
    tt: list = field(default_factory=list)
    bc: dict = field(default_factory=dict)
    bw: dict = field(default_factory=dict)
    be: dict = field(default_factory=dict)
    bd: dict = field(default_factory=dict)


def grade(tt, bank=10000.0):
    r = R(); r.tt = tt
    if not tt: return r
    r.n = len(tt)
    w = [t for t in tt if t.pnl > 0]
    l = [t for t in tt if t.pnl < 0]
    r.win = len(w); r.los = len(l)
    r.avg_w = sum(t.pnl for t in w) / len(w) if w else 0
    r.avg_l = sum(t.pnl for t in l) / len(l) if l else 0
    r.pnl = sum(t.pnl for t in tt)
    r.wr = r.win / r.n if r.n else 0
    gp = sum(t.pnl for t in w); gl = abs(sum(t.pnl for t in l))
    r.pf = gp / gl if gl else float("inf")
    r.ret = r.pnl / bank * 100
    rets = [t.pnl / bank for t in tt]
    if len(rets) > 1:
        m = sum(rets) / len(rets)
        v = sum((x - m) ** 2 for x in rets) / (len(rets) - 1)
        s = math.sqrt(v) if v else 0
        r.sharpe = (m / s) * math.sqrt(252) if s else (0 if m == 0 else float("inf"))
    cum = 0.0; peak = 0.0
    for t in tt:
        cum += t.pnl; peak = max(peak, cum)
        r.dd = max(r.dd, (peak - cum) / bank * 100)
    for t in tt:
        r.bc.setdefault(t.cat, {"t": 0, "p": 0.0, "w": 0})["t"] += 1
        r.bc[t.cat]["p"] += t.pnl
        if t.pnl > 0: r.bc[t.cat]["w"] += 1
        r.bw.setdefault(t.whale, {"t": 0, "p": 0.0, "w": 0})["t"] += 1
        r.bw[t.whale]["p"] += t.pnl
        if t.pnl > 0: r.bw[t.whale]["w"] += 1
        r.be.setdefault(t.reason, {"t": 0, "p": 0.0})["t"] += 1
        r.be[t.reason]["p"] += t.pnl
        d = t.entry[:10] if t.entry else "?"
        r.bd.setdefault(d, {"t": 0, "p": 0.0})["t"] += 1
        r.bd[d]["p"] += t.pnl
    s = 0
    if r.wr >= 0.60: s += 25
    elif r.wr >= 0.50: s += 15
    elif r.wr >= 0.40: s += 5
    if r.pf >= 2.0: s += 25
    elif r.pf >= 1.5: s += 15
    elif r.pf >= 1.0: s += 5
    if r.sharpe >= 2.0: s += 25
    elif r.sharpe >= 1.0: s += 15
    elif r.sharpe >= 0.5: s += 5
    if r.dd <= 5: s += 25
    elif r.dd <= 15: s += 15
    elif r.dd <= 25: s += 5
    if s >= 80: r.g = "A"
    elif s >= 60: r.g = "B"
    elif s >= 40: r.g = "C"
    elif s >= 20: r.g = "D"
    else: r.g = "F"
    r.gr = f"Score: {s}/100"
    return r


def report(r, src):
    l = [
        "# Strategy Grading Report",
        f"Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        f"Data source: {src}", "",
        f"## Overall Grade: {r.g}", f"{r.gr}", "",
        "## Performance Metrics", "",
        "| Metric | Value | Assessment |", "|--------|-------|------------|",
        f"| Total Trades | {r.n} | {'OK' if r.n >= 20 else 'Small'} |",
        f"| Win Rate | {r.wr:.1%} | {'OK' if r.wr >= 0.50 else 'Poor'} |",
        f"| Total PnL | ${r.pnl:,.2f} | {'PROFIT' if r.pnl > 0 else 'LOSS'} |",
        f"| Profit Factor | {r.pf:.2f} | {'OK' if r.pf >= 1.5 else 'Poor'} |",
        f"| Sharpe Ratio | {r.sharpe:.2f} | {'OK' if r.sharpe >= 1.0 else 'Poor'} |",
        f"| Max Drawdown | {r.dd:.1f}% | {'OK' if r.dd <= 5 else 'High'} |",
        f"| Avg Win | ${r.avg_w:,.2f} | |",
        f"| Avg Loss | $-{abs(r.avg_l):,.2f} | |",
        f"| Total Return | {r.ret:+.2f}% | |", "",
    ]
    if r.bd:
        l += ["## Daily PnL", "", "| Date | Trades | PnL |", "|------|--------|-----|"]
        for d, v in r.bd.items():
            l.append(f"| {d} | {v['t']} | ${v['p']:+,.2f} |")
        l.append("")
    if r.bc:
        l += ["## By Category", "", "| Category | Trades | PnL | WR |", "|----------|--------|-----|-----|"]
        for c, v in sorted(r.bc.items(), key=lambda x: x[1]["p"]):
            l.append(f"| {c} | {v['t']} | ${v['p']:+,.2f} | {v['w']/v['t']:.0%} |")
        l.append("")
    if r.be:
        l += ["## By Exit Reason", "", "| Reason | Trades | PnL |", "|--------|--------|-----|"]
        for re, v in sorted(r.be.items(), key=lambda x: x[1]["p"]):
            l.append(f"| {re} | {v['t']} | ${v['p']:+,.2f} |")
        l.append("")
    sw = sorted(r.bw.items(), key=lambda x: x[1]["p"])
    bad = [(w, d) for w, d in sw if d["p"] < 0 and d["t"] >= 5][:10]
    if bad:
        l += ["## Losing Whales (5+ trades)", "", "| Whale | Trades | PnL | Avg | WR |", "|-------|--------|-----|-----|-----|"]
        for w, d in bad:
            l.append(f"| {w[:30]} | {d['t']} | ${d['p']:+,.0f} | ${d['p']/d['t']:+,.0f} | {d['w']/d['t']:.0%} |")
        l.append("")
    good = [(w, d) for w, d in sw if d["p"] > 0 and d["t"] >= 5][-10:]
    if good:
        l += ["## Best Whales (5+ trades)", "", "| Whale | Trades | PnL | Avg | WR |", "|-------|--------|-----|-----|-----|"]
        for w, d in reversed(good):
            l.append(f"| {w[:30]} | {d['t']} | ${d['p']:+,.0f} | ${d['p']/d['t']:+,.0f} | {d['w']/d['t']:.0%} |")
        l.append("")
    l += ["## Recommendations", ""]
    if r.g in "AB": l.append("- Strategy OK.")
    elif r.g == "C": l.append("- Marginal. Review whale selection.")
    else: l.append("- Needs improvement. Blacklist losers.")
    bb = [(w, d) for w, d in sw if d["p"] < -1000 and d["t"] >= 10]
    if bb: l.append(f"- Blacklist: {', '.join(w[:20] for w, _ in bb[:3])}")
    l.append("")
    return "\n".join(l)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--output", default="")
    p.add_argument("--bankroll", type=float, default=10000.0)
    p.add_argument("--days", type=int, default=0)
    p.add_argument("--json", default="")
    a = p.parse_args()
    OUT.mkdir(parents=True, exist_ok=True)
    tt = fetch(a.days)
    if not tt: print("No trades found"); return 1
    src = f"trades.db ({len(tt)} trades" + (f", {a.days}d" if a.days else ", all") + ")"
    g = grade(tt, a.bankroll)
    rpt = report(g, src)
    out = a.output or str(OUT / f"grade-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M')}.md")
    Path(out).write_text(rpt); print(f"Saved: {out}")
    if a.json:
        s = {"grade": g.g, "trades": g.n, "pnl": round(g.pnl, 2), "wr": round(g.wr, 4), "pf": round(g.pf, 2), "sharpe": round(g.sharpe, 2), "dd": round(g.dd, 2)}
        Path(a.json).write_text(json.dumps(s, indent=2)); print(f"JSON: {a.json}")
    print(rpt)
    return 0

if __name__ == "__main__":
    sys.exit(main())
