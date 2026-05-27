"""Hermes Dashboard Server -- FastAPI + WebSocket live observability.

Provides real-time metrics, whale rankings, trade history, risk status,
slippage tracking, and system health for the Hermes trading system.

Runs as a separate service alongside the trading strategy:
    python -m components.dashboard.server

Endpoints:
    GET  /              -- Dashboard HTML
    GET  /api/overview  -- System overview metrics
    GET  /api/whales    -- Whale rankings and classifications
    GET  /api/trades    -- Recent trade history
    GET  /api/risk      -- Risk state and limits
    GET  /api/performance -- Category and regime performance
    GET  /api/positions -- Open positions snapshot
    WS   /ws/live       -- WebSocket for live updates
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
import threading
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware

logger_cb = __import__("logging").getLogger("DashboardServer")

DB_PATH = Path("/home/elon-1/workspace/nautilus-trading/data/trades.db")
CLASSIFICATIONS_PATH = Path("/home/elon-1/workspace/nautilus-trading/data/whale_classifications.json")
DYNAMIC_STATE_PATH = Path("/home/elon-1/workspace/nautilus-trading/data/dynamic_whale_state.json")
ADAPTIVE_STATE_PATH = Path("/home/elon-1/workspace/nautilus-trading/data/adaptive_intel_state.json")
REGIME_STATE_PATH = Path("/home/elon-1/workspace/nautilus-trading/data/regime_state.json")
ADVERSARIAL_STATE_PATH = Path("/home/elon-1/workspace/nautilus-trading/data/adversarial_state.json")

app = FastAPI(title="Hermes Dashboard", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Database Queries ──────────────────────────────────────────────────────────

def _get_db():
    conn = sqlite3.connect(str(DB_PATH), timeout=30.0)
    conn.row_factory = sqlite3.Row
    return conn


def _query_overview() -> dict:
    """System overview: total trades, PnL, win rate, active positions."""
    conn = _get_db()
    try:
        row = conn.execute("""
            SELECT COUNT(*) as total_trades,
                   ROUND(SUM(CASE WHEN realized_pnl > 0 THEN 1.0 ELSE 0.0 END) / MAX(COUNT(*), 1), 4) as win_rate,
                   ROUND(SUM(realized_pnl), 2) as total_pnl,
                   ROUND(AVG(realized_pnl), 2) as avg_pnl,
                   COUNT(CASE WHEN exit_reason IS NULL THEN 1 END) as open_positions,
                   COUNT(CASE WHEN realized_pnl > 0 THEN 1 END) as wins,
                   COUNT(CASE WHEN realized_pnl <= 0 THEN 1 END) as losses
            FROM trades
            WHERE realized_pnl IS NOT NULL
        """).fetchone()

        # Today's PnL
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        today_row = conn.execute("""
            SELECT ROUND(SUM(realized_pnl), 2) as today_pnl,
                   COUNT(*) as today_trades
            FROM trades
            WHERE DATE(timestamp) = ? AND realized_pnl IS NOT NULL
        """, (today,)).fetchone()

        # Category breakdown
        cat_rows = conn.execute("""
            SELECT category,
                   COUNT(*) as trades,
                   ROUND(SUM(CASE WHEN realized_pnl > 0 THEN 1.0 ELSE 0.0 END) / MAX(COUNT(*), 1), 4) as win_rate,
                   ROUND(SUM(realized_pnl), 2) as pnl
            FROM trades
            WHERE realized_pnl IS NOT NULL
            GROUP BY category
            ORDER BY pnl DESC
        """).fetchall()

        return {
            "total_trades": row["total_trades"],
            "win_rate": row["win_rate"],
            "total_pnl": row["total_pnl"],
            "avg_pnl": row["avg_pnl"],
            "open_positions": row["open_positions"],
            "wins": row["wins"],
            "losses": row["losses"],
            "today_pnl": today_row["today_pnl"] or 0,
            "today_trades": today_row["today_trades"] or 0,
            "categories": [dict(r) for r in cat_rows],
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
    finally:
        conn.close()


def _query_whales() -> list[dict]:
    """Whale rankings with classifications and trust scores."""
    # Load dynamic whale intel if available
    dynamic_data = {}
    if DYNAMIC_STATE_PATH.exists():
        try:
            dynamic_data = json.loads(DYNAMIC_STATE_PATH.read_text()).get("whales", {})
        except Exception:
            pass

    # Load static classifications
    static_data = {}
    if CLASSIFICATIONS_PATH.exists():
        try:
            static_data = json.loads(CLASSIFICATIONS_PATH.read_text()).get("classifications", {})
        except Exception:
            pass

    # Merge with DB performance
    conn = _get_db()
    try:
        rows = conn.execute("""
            SELECT whale_name,
                   COUNT(*) as trades,
                   ROUND(SUM(CASE WHEN realized_pnl > 0 THEN 1.0 ELSE 0.0 END) / MAX(COUNT(*), 1), 4) as win_rate,
                   ROUND(SUM(realized_pnl), 2) as total_pnl,
                   ROUND(AVG(realized_pnl), 2) as avg_pnl,
                   ROUND(AVG(confidence), 3) as avg_confidence,
                   ROUND(AVG(edge_score), 3) as avg_edge
            FROM trades
            WHERE realized_pnl IS NOT NULL
              AND whale_name IS NOT NULL
              AND whale_name != 'autoresearch_llm'
            GROUP BY whale_name
            HAVING COUNT(*) >= 3
            ORDER BY total_pnl DESC
        """).fetchall()

        whales = []
        for row in rows:
            name = row["whale_name"]
            dynamic = dynamic_data.get(name, {})
            static = static_data.get(name, {})
            whales.append({
                "name": name,
                "trades": row["trades"],
                "win_rate": row["win_rate"],
                "total_pnl": row["total_pnl"],
                "avg_pnl": row["avg_pnl"],
                "avg_confidence": row["avg_confidence"],
                "avg_edge": row["avg_edge"],
                "classification": dynamic.get("classification") or static.get("classification", "unknown"),
                "action": dynamic.get("action") or static.get("action", "ignore"),
                "trust": dynamic.get("overall_trust", static.get("trust", 5.0)),
                "recent_wr": dynamic.get("recent_wr", 0.0),
            })
        return whales
    finally:
        conn.close()


def _query_trades(limit: int = 50, offset: int = 0) -> list[dict]:
    """Recent trade history with PnL and resolution."""
    conn = _get_db()
    try:
        rows = conn.execute("""
            SELECT trade_id, timestamp, whale_name, category, market_title,
                   side, entry_price, exit_price, position_size_usd,
                   confidence, edge_score, signal_source,
                   entry_reason, exit_reason,
                   realized_pnl, actual_return, duration_seconds,
                   resolution_outcome, dispute_flag, instrument_id
            FROM trades
            ORDER BY timestamp DESC
            LIMIT ? OFFSET ?
        """, (limit, offset)).fetchall()

        return [dict(r) for r in rows]
    finally:
        conn.close()


def _query_risk() -> dict:
    """Risk state: daily PnL, kill switch status, position limits."""
    # Load daily state
    daily_state_path = Path("/home/elon-1/workspace/nautilus-trading/data/daily_state.json")
    daily_state = {}
    if daily_state_path.exists():
        try:
            daily_state = json.loads(daily_state_path.read_text())
        except Exception:
            pass

    conn = _get_db()
    try:
        # Current exposure
        open_rows = conn.execute("""
            SELECT ROUND(SUM(position_size_usd), 2) as total_exposure,
                   COUNT(*) as open_count
            FROM trades
            WHERE exit_reason IS NULL AND realized_pnl IS NULL
        """).fetchone()

        # Category exposure
        cat_exposure = conn.execute("""
            SELECT category,
                   ROUND(SUM(position_size_usd), 2) as exposure,
                   COUNT(*) as count
            FROM trades
            WHERE exit_reason IS NULL AND realized_pnl IS NULL
            GROUP BY category
        """).fetchall()

        return {
            "daily_pnl": daily_state.get("daily_pnl", 0),
            "daily_loss_breached": daily_state.get("daily_loss_breached", False),
            "kill_switch_active": daily_state.get("daily_loss_breached", False),
            "total_exposure": dict(open_rows).get("total_exposure", 0) if open_rows else 0,
            "open_positions": dict(open_rows).get("open_count", 0) if open_rows else 0,
            "category_exposure": [dict(r) for r in cat_exposure],
        }
    finally:
        conn.close()


def _query_performance() -> dict:
    """Category performance and adaptive intelligence state."""
    # Load adaptive intel state
    adaptive_data = {}
    if ADAPTIVE_STATE_PATH.exists():
        try:
            adaptive_data = json.loads(ADAPTIVE_STATE_PATH.read_text())
        except Exception:
            pass

    categories = adaptive_data.get("category_perf", {})

    # Enrich with DB data
    conn = _get_db()
    try:
        rows = conn.execute("""
            SELECT category,
                   COUNT(*) as trades,
                   ROUND(SUM(CASE WHEN realized_pnl > 0 THEN 1.0 ELSE 0.0 END) / MAX(COUNT(*), 1), 4) as win_rate,
                   ROUND(SUM(realized_pnl), 2) as pnl,
                   ROUND(AVG(realized_pnl), 2) as avg_pnl
            FROM trades
            WHERE realized_pnl IS NOT NULL AND category IS NOT NULL
            GROUP BY category
            ORDER BY pnl DESC
        """).fetchall()

        db_cats = {r["category"]: dict(r) for r in rows}
        for cat, data in categories.items():
            if cat in db_cats:
                data["db_trades"] = db_cats[cat]["trades"]
                data["db_pnl"] = db_cats[cat]["pnl"]
                data["db_win_rate"] = db_cats[cat]["win_rate"]

        return {
            "categories": categories,
            "last_update": adaptive_data.get("last_update"),
            "whale_regime_count": adaptive_data.get("whale_regime_count", 0),
        }
    finally:
        conn.close()


def _query_positions() -> list[dict]:
    """Open positions snapshot."""
    conn = _get_db()
    try:
        rows = conn.execute("""
            SELECT instrument_id, whale_name, market_title, category, side,
                   entry_price, position_size_usd, confidence, edge_score,
                   signal_source, entry_reason, timestamp, condition_id
            FROM trades
            WHERE exit_reason IS NULL
            ORDER BY timestamp DESC
        """).fetchall()

        return [dict(r) for r in rows]
    finally:
        conn.close()


# ── API Endpoints ─────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def dashboard():
    """Serve the dashboard HTML."""
    return HTMLResponse(DASHBOARD_HTML)


@app.get("/api/overview")
async def api_overview():
    """System overview metrics."""
    return _query_overview()


@app.get("/api/whales")
async def api_whales(limit: int = 50, action: str = ""):
    """Whale rankings and classifications."""
    whales = _query_whales()
    if action:
        whales = [w for w in whales if w["action"] == action]
    return whales[:limit]


@app.get("/api/trades")
async def api_trades(limit: int = 50, offset: int = 0, category: str = ""):
    """Recent trade history."""
    trades = _query_trades(limit=limit, offset=offset)
    if category:
        trades = [t for t in trades if t.get("category", "").lower() == category.lower()]
    return trades


@app.get("/api/risk")
async def api_risk():
    """Risk state and limits."""
    return _query_risk()


@app.get("/api/performance")
async def api_performance():
    """Category and regime performance."""
    return _query_performance()


@app.get("/api/positions")
async def api_positions():
    """Open positions snapshot."""
    return _query_positions()


@app.get("/api/adversarial")
async def api_adversarial():
    """Adversarial whale classifications and regime states."""
    result = {"adversarial": [], "regimes": []}
    
    # Load adversarial classifications
    if ADVERSARIAL_STATE_PATH.exists():
        try:
            adv_data = json.loads(ADVERSARIAL_STATE_PATH.read_text())
            classifications = adv_data.get("whale_classifications", {})
            for name, info in classifications.items():
                if info.get("is_adversarial"):
                    result["adversarial"].append({
                        "name": name,
                        "type": info.get("adversarial_type", "unknown"),
                        "confidence": info.get("confidence", 0),
                        "recommendation": info.get("recommendation", ""),
                        "reason": info.get("reason", ""),
                        "trades": info.get("metrics", {}).get("overall_trades", 0),
                        "wr": info.get("metrics", {}).get("overall_wr", 0),
                        "pnl": info.get("metrics", {}).get("overall_pnl", 0),
                        "avg_size": info.get("metrics", {}).get("avg_size", 0),
                    })
        except Exception:
            pass
    
    # Load regime states
    if REGIME_STATE_PATH.exists():
        try:
            regime_data = json.loads(REGIME_STATE_PATH.read_text())
            regimes = regime_data.get("whale_regimes", {})
            for key, info in regimes.items():
                if info.get("loss_leader_suspected") or info.get("regime_change_detected") or info.get("adversarial_suspected"):
                    result["regimes"].append({
                        "key": key,
                        "whale": info.get("whale_name", ""),
                        "category": info.get("category", ""),
                        "side": info.get("side", ""),
                        "regime_type": info.get("regime_type", "stable"),
                        "loss_leader": info.get("loss_leader_suspected", False),
                        "regime_change": info.get("regime_change_detected", False),
                        "adversarial": info.get("adversarial_suspected", False),
                        "recent_wr": info.get("recent_wr", 0),
                        "historical_wr": info.get("historical_wr", 0),
                        "reason": info.get("reason", ""),
                    })
        except Exception:
            pass
    
    return result


@app.get("/api/slippage")
async def api_slippage(limit: int = 100):
    """Slippage and latency metrics."""
    conn = _get_db()
    try:
        rows = conn.execute("""
            SELECT whale_name, category, side,
                   intended_entry_price, actual_fill_price,
                   slippage_bps, fill_completion_pct,
                   detection_delay_ms, execution_delay_ms, fill_delay_ms, total_latency_ms,
                   timestamp
            FROM trades
            WHERE slippage_bps IS NOT NULL AND slippage_bps != 0
            ORDER BY timestamp DESC
            LIMIT ?
        """, (limit,)).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


@app.get("/api/health")
async def api_health():
    """Health check endpoint."""
    return {"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()}


# ── WebSocket for Live Updates ────────────────────────────────────────────────

class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception:
                pass


manager = ConnectionManager()


@app.websocket("/ws/live")
async def websocket_live(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            # Send overview every 5 seconds
            overview = _query_overview()
            await websocket.send_json({"type": "overview", "data": overview})
            await asyncio.sleep(5)
    except WebSocketDisconnect:
        manager.disconnect(websocket)


# ── Dashboard HTML ────────────────────────────────────────────────────────────

DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Hermes Dashboard</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:#0a0e17;color:#e1e5eb;padding:20px}
.container{max-width:1400px;margin:0 auto}
header{display:flex;align-items:center;justify-content:space-between;padding:16px 0;border-bottom:1px solid #1e2736;margin-bottom:24px}
header h1{font-size:22px;font-weight:600;color:#fff;letter-spacing:-0.5px}
header .status{display:flex;align-items:center;gap:8px;font-size:13px;color:#8b95a5}
header .dot{width:8px;height:8px;border-radius:50%;background:#22c55e;animation:pulse 2s infinite}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.5}}
.grid{display:grid;grid-template-columns:repeat(4,1fr);gap:16px;margin-bottom:24px}
.card{background:#111827;border:1px solid #1e2736;border-radius:8px;padding:20px}
.card .label{font-size:12px;color:#6b7280;text-transform:uppercase;letter-spacing:.5px;margin-bottom:8px}
.card .value{font-size:28px;font-weight:700;color:#fff}
.card .sub{font-size:13px;color:#6b7280;margin-top:4px}
.card.positive .value{color:#22c55e}
.card.negative .value{color:#ef4444}
.section{margin-bottom:24px}
.section h2{font-size:16px;font-weight:600;color:#fff;margin-bottom:12px}
table{width:100%;border-collapse:collapse}
th{text-align:left;font-size:11px;color:#6b7280;text-transform:uppercase;letter-spacing:.5px;padding:8px 12px;border-bottom:1px solid #1e2736}
td{font-size:13px;padding:8px 12px;border-bottom:1px solid #111827}
tr:hover{background:#1e273620}
.badge{display:inline-block;padding:2px 8px;border-radius:4px;font-size:11px;font-weight:600}
.badge.copy{background:#22c55e20;color:#22c55e}
.badge.fade{background:#ef444420;color:#ef4444}
.badge.ignore{background:#6b728020;color:#6b7280}
.badge.skilled{background:#3b82f620;color:#3b82f6}
.badge.degen{background:#f59e0b20;color:#f59e0b}
.badge.sac{background:#ef444420;color:#ef4444}
.badge.mixed{background:#8b5cf620;color:#8b5cf6}
.badge.bot{background:#06b6d420;color:#06b6d4}
.regime{font-size:11px;padding:2px 6px;border-radius:3px}
.regime.trending{background:#22c55e20;color:#22c55e}
.regime.neutral{background:#6b728020;color:#6b7280}
.regime.volatile{background:#f59e0b20;color:#f59e0b}
.regime.crisis{background:#ef444420;color:#ef4444}
#ws-status{font-size:11px;color:#6b7280;margin-left:8px}
</style>
</head>
<body>
<div class="container">
<header>
  <h1>Hermes Dashboard</h1>
  <div class="status"><span class="dot"></span><span id="status">Loading...</span><span id="ws-status"></span></div>
</header>

<div class="grid" id="overview">
  <div class="card"><div class="label">Total PnL</div><div class="value" id="total-pnl">--</div><div class="sub" id="total-trades">-- trades</div></div>
  <div class="card"><div class="label">Win Rate</div><div class="value" id="win-rate">--</div><div class="sub" id="wins-losses">--</div></div>
  <div class="card"><div class="label">Open Positions</div><div class="value" id="open-pos">--</div><div class="sub" id="exposure">--</div></div>
  <div class="card"><div class="label">Today PnL</div><div class="value" id="today-pnl">--</div><div class="sub" id="today-trades">-- trades today</div></div>
</div>

<div class="section">
<h2>Category Performance</h2>
<div id="categories"><table><thead><tr><th>Category</th><th>Trades</th><th>Win Rate</th><th>PnL</th><th>Regime</th><th>Weight</th><th>Kelly Mult</th></tr></thead><tbody id="cat-tbody"></tbody></table></div>
</div>

<div class="section">
<h2>Whale Rankings</h2>
<table><thead><tr><th>Whale</th><th>Trades</th><th>WR</th><th>PnL</th><th>Trust</th><th>Classification</th><th>Action</th><th>Recent WR</th></tr></thead><tbody id="whale-tbody"></tbody></table>
</div>

<div class="section">
<h2>Open Positions</h2>
<table><thead><tr><th>Market</th><th>Whale</th><th>Side</th><th>Entry</th><th>Size</th><th>Category</th><th>Edge</th><th>Confidence</th></tr></thead><tbody id="pos-tbody"></tbody></table>
</div>

<div class="section">
<h2>Recent Trades</h2>
<table><thead><tr><th>Time</th><th>Whale</th><th>Market</th><th>Side</th><th>PnL</th><th>Category</th><th>Edge</th></tr></thead><tbody id="trades-tbody"></tbody></table>
</div>

<div class="section">
<h2>Adversarial &amp; Regime Intelligence</h2>
<table><thead><tr><th>Whale</th><th>Type</th><th>Confidence</th><th>Recommendation</th><th>Reason</th><th>Trades</th><th>WR</th><th>PnL</th></tr></thead><tbody id="adv-tbody"></tbody></table>
<h3 style="margin-top:16px;color:#f59e0b">Regime Alerts</h3>
<table><thead><tr><th>Whale</th><th>Category</th><th>Side</th><th>Regime</th><th>Loss Leader</th><th>Behavior Shift</th><th>Recent WR</th><th>Historical WR</th></tr></thead><tbody id="regime-tbody"></tbody></table>
</div>
</div>

<script>
const fmt = (n,d=2) => n != null ? Number(n).toFixed(d) : '--';
const pnlClass = (n) => n > 0 ? 'positive' : n < 0 ? 'negative' : '';
const actionBadge = (a) => '<span class="badge ' + a + '">' + a + '</span>';
const classBadge = (c) => {
  const map = {skilled_human:'skilled',degenerate_human:'degen',sacrificial_account:'sac',mixed_entity:'mixed',trading_bot:'bot'};
  return '<span class="badge ' + (map[c]||'mixed') + '">' + c + '</span>';
};

async function loadOverview() {
  try {
    const r = await fetch('/api/overview');
    const d = await r.json();
    const pnl = d.total_pnl || 0;
    document.getElementById('total-pnl').textContent = '$' + fmt(pnl);
    document.getElementById('total-pnl').className = 'value ' + pnlClass(pnl);
    document.getElementById('total-trades').textContent = (d.total_trades||0) + ' trades';
    document.getElementById('win-rate').textContent = fmt((d.win_rate||0)*100,1) + '%';
    document.getElementById('wins-losses').textContent = (d.wins||0) + 'W / ' + (d.losses||0) + 'L';
    document.getElementById('open-pos').textContent = d.open_positions || 0;
    document.getElementById('exposure').textContent = 'Exposure: $' + fmt(d.total_pnl);
    const tpnl = d.today_pnl || 0;
    document.getElementById('today-pnl').textContent = '$' + fmt(tpnl);
    document.getElementById('today-pnl').className = 'value ' + pnlClass(tpnl);
    document.getElementById('today-trades').textContent = (d.today_trades||0) + ' trades today';
    document.getElementById('status').textContent = 'Live';
  } catch(e) { document.getElementById('status').textContent = 'Error: ' + e.message; }
}

async function loadCategories() {
  try {
    const r = await fetch('/api/performance');
    const d = await r.json();
    const cats = d.categories || {};
    const tbody = document.getElementById('cat-tbody');
    tbody.innerHTML = '';
    for (const [name, c] of Object.entries(cats)) {
      const regimeClass = (c.regime||'neutral').toLowerCase();
      tbody.innerHTML += '<tr><td>' + name + '</td><td>' + (c.db_trades||c.trade_count||0) + '</td><td>' + fmt((c.db_win_rate||c.win_rate||0)*100,1) + '%</td><td>$' + fmt(c.db_pnl||c.total_pnl||0) + '</td><td><span class="regime ' + regimeClass + '">' + (c.regime||'neutral') + '</span></td><td>' + fmt(c.weight||0,3) + '</td><td>' + fmt(c.kelly_multiplier||1,2) + '</td></tr>';
    }
  } catch(e) {}
}

async function loadAdversarial() {
  try {
    const r = await fetch('/api/adversarial');
    const d = await r.json();
    const advTbody = document.getElementById('adv-tbody');
    advTbody.innerHTML = '';
    for (const w of (d.adversarial || [])) {
      const typeBadge = '<span class="badge ' + (w.type === 'market_maker' ? 'bot' : w.type === 'loss_leader' ? 'degen' : w.type === 'sacrificial' ? 'sac' : 'mixed') + '">' + w.type + '</span>';
      const recBadge = '<span class="badge ' + (w.recommendation === 'ignore' ? 'ignore' : w.recommendation === 'no_fade' ? 'fade' : 'copy') + '">' + w.recommendation + '</span>';
      advTbody.innerHTML += '<tr><td>' + w.name + '</td><td>' + typeBadge + '</td><td>' + fmt(w.confidence * 100, 0) + '%</td><td>' + recBadge + '</td><td style="font-size:11px;max-width:300px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">' + (w.reason || '') + '</td><td>' + w.trades + '</td><td>' + fmt(w.wr * 100, 1) + '%</td><td>$' + fmt(w.pnl) + '</td></tr>';
    }
    const regimeTbody = document.getElementById('regime-tbody');
    regimeTbody.innerHTML = '';
    for (const r of (d.regimes || [])) {
      const llBadge = r.loss_leader ? '<span class="badge degen">YES</span>' : 'no';
      const shiftBadge = r.regime_change ? '<span class="badge sac">YES</span>' : 'no';
      regimeTbody.innerHTML += '<tr><td>' + r.whale + '</td><td>' + r.category + '</td><td>' + r.side + '</td><td>' + r.regime_type + '</td><td>' + llBadge + '</td><td>' + shiftBadge + '</td><td>' + fmt(r.recent_wr * 100, 1) + '%</td><td>' + fmt(r.historical_wr * 100, 1) + '%</td></tr>';
    }
  } catch(e) { console.error('Adversarial load error:', e); }
}

async function loadWhales() {
  try {
    const r = await fetch('/api/whales?limit=30');
    const whales = await r.json();
    const tbody = document.getElementById('whale-tbody');
    tbody.innerHTML = '';
    for (const w of whales) {
      const cls = pnlClass(w.total_pnl);
      tbody.innerHTML += '<tr><td>' + w.name + '</td><td>' + w.trades + '</td><td>' + fmt(w.win_rate*100,1) + '%</td><td class="' + cls + '">$' + fmt(w.total_pnl) + '</td><td>' + fmt(w.trust,1) + '</td><td>' + classBadge(w.classification) + '</td><td>' + actionBadge(w.action) + '</td><td>' + fmt((w.recent_wr||0)*100,1) + '%</td></tr>';
    }
  } catch(e) {}
}

async function loadPositions() {
  try {
    const r = await fetch('/api/positions');
    const pos = await r.json();
    const tbody = document.getElementById('pos-tbody');
    tbody.innerHTML = '';
    for (const p of pos) {
      tbody.innerHTML += '<tr><td>' + (p.market_title||'').substring(0,40) + '</td><td>' + (p.whale_name||'') + '</td><td>' + (p.side||'') + '</td><td>' + fmt(p.entry_price) + '</td><td>$' + fmt(p.position_size_usd) + '</td><td>' + (p.category||'') + '</td><td>' + fmt(p.edge_score,3) + '</td><td>' + fmt(p.confidence,2) + '</td></tr>';
    }
  } catch(e) {}
}

async function loadTrades() {
  try {
    const r = await fetch('/api/trades?limit=20');
    const trades = await r.json();
    const tbody = document.getElementById('trades-tbody');
    tbody.innerHTML = '';
    for (const t of trades) {
      const cls = pnlClass(t.realized_pnl);
      tbody.innerHTML += '<tr><td>' + (t.timestamp||'').substring(0,16) + '</td><td>' + (t.whale_name||'').substring(0,20) + '</td><td>' + (t.market_title||'').substring(0,35) + '</td><td>' + (t.side||'') + '</td><td class="' + cls + '">$' + fmt(t.realized_pnl) + '</td><td>' + (t.category||'') + '</td><td>' + fmt(t.edge_score,3) + '</td></tr>';
    }
  } catch(e) {}
}

async function refresh() {
  await Promise.all([loadOverview(), loadCategories(), loadWhales(), loadPositions(), loadTrades()]);
}

refresh();
setInterval(refresh, 10000);
</script>
</body>
</html>"""


def run_server(host: str = "127.0.0.1", port: int = 8721):
    """Run the dashboard server."""
    import uvicorn
    uvicorn.run(app, host=host, port=port, log_level="info")


if __name__ == "__main__":
    run_server()
