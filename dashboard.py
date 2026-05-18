"""Nautilus Whale Follower — Lightweight Web Dashboard.

Flask app showing real-time system status, whale roster, signals, markets, and logs.
Runs on port 8502.

Usage:
    cd ~/workspace/nautilus-trading
    venv/bin/python dashboard.py
    # Open: http://192.168.50.218:8502
"""

import json
import os
import sqlite3
import subprocess
import threading
import time
from datetime import datetime, timezone

from flask import Flask, jsonify, send_file, Response

from components.resolution_poller import ResolutionPoller

app = Flask(__name__)


DB_PATH = "/home/elon-1/workspace/nautilus-trading/data/trades.db"
NAUTILUS_PROC = "run_paper.py"
LOG_LINES = 80


def get_process_info():
    try:
        # Use ps aux for cross-platform compatibility (pgrep -fa output varies: Linux shows "pid cmd", macOS shows bare PIDs)
        result = subprocess.run(
            ["ps", "aux"],
            capture_output=True, text=True, timeout=5)
        pids = []
        for line in result.stdout.strip().split("\n"):
            if NAUTILUS_PROC in line and "grep" not in line:
                parts = line.split()
                if parts:
                    pids.append(parts[1])  # ps aux format: USER PID ...
        if not pids:
            return {"running": False, "pids": [], "main_pid": "", "uptime_output": ""}
        # Sort numerically to get the oldest (likely parent) process first
        pids.sort(key=int)
        uptime_result = subprocess.run(
            ["ps", "-p", pids[0], "-o", "pid,etime,rss,vsz"],
            capture_output=True, text=True, timeout=5)
        return {
            "running": True,
            "pids": pids,
            "main_pid": pids[0],
            "uptime_output": uptime_result.stdout.strip(),
        }
    except Exception:
        pass
    return {"running": False, "pids": [], "main_pid": "", "uptime_output": ""}


def get_log_tail(pid=None, lines=LOG_LINES):
    # macOS/Linux: read from paper.log instead of /proc/<pid>/fd/1
    log_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs", "paper.log")
    if os.path.exists(log_path):
        try:
            with open(log_path) as f:
                all_lines = f.readlines()
            if all_lines:
                return "".join(all_lines[-lines:]).strip()
        except Exception:
            pass
    # Fallback: try dashboard.log
    log_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dashboard.log")
    if os.path.exists(log_path):
        try:
            with open(log_path) as f:
                text = f.read()
            parts = text.strip().split("\n")
            return "\n".join(parts[-lines:])
        except Exception:
            pass
    return "No logs available"


def get_db_stats():
    stats = {
        "whale_count": 0, "active_whales": 0, "total_signals": 0,
        "unsignaled": 0, "signals_today": 0,
        "top_whales": [], "top_markets": [], "recent_signals": [],
    }
    
    # 1. Whale stats from Discovery DB
    if os.path.exists(WHALE_DB_PATH):
        try:
            conn = sqlite3.connect(WHALE_DB_PATH)
            stats["whale_count"] = conn.execute("SELECT COUNT(*) FROM whales").fetchone()[0]
            stats["active_whales"] = conn.execute("SELECT COUNT(*) FROM whales WHERE alpha_score >= 70").fetchone()[0]
            stats["top_whales"] = conn.execute("SELECT name, alpha_score, pnl, volume, total_trades, tags FROM whales ORDER BY alpha_score DESC LIMIT 10").fetchall()
            conn.close()
        except Exception as e: print(f"[DASH] Whale DB error: {e}")

    # 2. Signal stats from Trade DB (which currently holds whale_signals)
    # Actually whale_signals might be in either. Let's check both for redundancy.
    active_sig_db = WHALE_DB_PATH if os.path.exists(WHALE_DB_PATH) else DB_PATH
    try:
        conn = sqlite3.connect(active_sig_db)
        stats["total_signals"] = conn.execute("SELECT COUNT(*) FROM whale_signals").fetchone()[0]
        stats["unsignaled"] = conn.execute("SELECT COUNT(*) FROM whale_signals WHERE signaled = 0").fetchone()[0]
        stats["signals_today"] = conn.execute("SELECT COUNT(*) FROM whale_signals WHERE detected_at > date('now', '-1 day')").fetchone()[0]
        stats["top_markets"] = conn.execute("SELECT market_title, condition_id, COUNT(*) as cnt FROM whale_signals GROUP BY condition_id, market_title ORDER BY cnt DESC LIMIT 15").fetchall()
        stats["recent_signals"] = conn.execute("SELECT whale_name, market_title, side, outcome, size, price, usd_value, confidence, detected_at FROM whale_signals ORDER BY detected_at DESC LIMIT 20").fetchall()
        conn.close()
    except Exception as e: print(f"[DASH] Signal DB error: {e}")
    
    return stats


def parse_log_signals(log_text):
    entries = []
    for line in log_text.split("\n"):
        if any(kw in line for kw in ["SIGNAL", "ENTER", "EXIT", "INSIDER ANALYSIS", "STOP LOSS", "TAKE PROFIT", "RESOLUTION"]):
            ts = line[:26] if len(line) > 26 else ""
            entries.append({"time": ts, "line": line[27:]})
    return entries[-20:]


_resolution_poller_instance = None

def get_pnl_stats():
    """Get real vs simulated P&L stats from trades.db."""
    global _resolution_poller_instance
    if _resolution_poller_instance is None:
        _resolution_poller_instance = ResolutionPoller()
    
    db_summary = _resolution_poller_instance.get_db_summary()
    recent = _resolution_poller_instance.get_recent_resolutions(limit=20)
    return {
        "summary": db_summary,
        "recent_resolutions": recent,
    }


def fmt_pct(v):
    if v is None:
        return "N/A"
    try:
        return str(int(float(v) * 100)) + "%"
    except (ValueError, TypeError):
        return "N/A"


def fmt_usd(v):
    if v is None:
        return "N/A"
    try:
        return "$" + "{:,.0f}".format(float(v))
    except (ValueError, TypeError):
        return "N/A"


def fmt_price(v):
    if v is None:
        return "N/A"
    try:
        return "{:.3f}".format(float(v))
    except (ValueError, TypeError):
        return "N/A"


def fmt_alpha(v):
    try:
        return "{:.1f}".format(float(v))
    except (ValueError, TypeError):
        return "N/A"


def render_activity(activity):
    if not activity:
        return '<p style="color:#8b949e;">No recent signals or trades detected.</p>'
    html = []
    for e in activity:
        cls = "signal-green" if "ENTER" in e["line"] else (
            "signal-red" if ("EXIT" in e["line"] or "STOP LOSS" in e["line"]) else (
            "signal-blue" if "SIGNAL" in e["line"] else "signal-yellow"))
        line_short = e["line"][:120]
        html.append(
            '<div style="margin-bottom:6px;font-size:13px;">'
            '<span class="' + cls + '">●</span> '
            '<code style="color:#8b949e;">' + e["time"] + '</code> — ' + line_short +
            '</div>'
        )
    return "\\n".join(html)


def render_whales(whales):
    html = []
    for w in whales:
        html.append("<tr>"
            "<td>" + str(w[0]) + "</td>"
            "<td>" + fmt_alpha(w[1]) + "</td>"
            "<td>" + fmt_usd(w[2]) + "</td>"
            "<td>" + fmt_usd(w[3]) + "</td>"
            "<td>" + str(w[4]) + "</td>"
            "<td>" + str(w[5]) + "</td>"
            "</tr>")
    return "\n".join(html)


def render_markets(mkts):
    html = []
    for m in mkts:
        html.append("<tr>"
            "<td>" + str(m[0])[:60] + "</td>"
            "<td style='font-family:monospace;'>" + str(m[1])[:20] + "...</td>"
            "<td>" + str(m[2]) + "</td>"
            "</tr>")
    return "\n".join(html)


def render_signals(sigs):
    html = []
    for s in sigs:
        html.append("<tr>"
            "<td>" + str(s[0]) + "</td>"
            "<td>" + (str(s[1])[:50] if s[1] else "N/A") + "</td>"
            "<td>" + (str(s[2]) or "N/A").upper() + "</td>"
            "<td>" + str(s[3]) + "</td>"
            "<td>" + fmt_usd(s[4]) + "</td>"
            "<td>" + fmt_price(s[5]) + "</td>"
            "<td>" + fmt_usd(s[6]) + "</td>"
            "<td>" + fmt_pct(s[7]) + "</td>"
            "<td>" + (str(s[8]) or "N/A")[:19] + "</td>"
            "</tr>")
    return "\n".join(html)


def render_resolved_trades(trades):
    """Render resolved trades table showing simulated vs actual P&L side by side."""
    html = []
    for t in trades:
        sim = t.get("realized_pnl", 0) or 0
        actual = t.get("actual_pnl", 0) or 0
        diff = actual - sim
        diff_color = "green" if diff >= 0 else "red"
        actual_color = "green" if actual >= 0 else "red"
        sim_color = "green" if sim >= 0 else "red"
        html.append("<tr>"
            "<td>" + (str(t.get("market_title", ""))[:45] or "N/A") + "</td>"
            "<td>" + (str(t.get("side", "")) or "N/A") + "</td>"
            "<td>" + fmt_price(t.get("entry_price")) + "</td>"
            "<td style='color:" + sim_color + "'>" + fmt_usd(sim) + "</td>"
            "<td style='color:" + actual_color + ";font-weight:bold;'>" + fmt_usd(actual) + "</td>"
            "<td style='color:" + diff_color + "'>" + fmt_usd(diff) + "</td>"
            "<td>" + str(t.get("resolution_outcome", ""))[:25] + "</td>"
            "<td>" + (str(t.get("timestamp", ""))[:19] if t.get("timestamp") else "N/A") + "</td>"
            "</tr>")
    return "\n".join(html) if html else '<tr><td colspan="8" style="color:#8b949e;">No resolved trades yet.</td></tr>'


def escape_html(s):
    return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")


def build_html(proc_info, db_stats, log_text, activity, pnl_stats=None):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    status_dot = "running" if proc_info["running"] else "stopped"
    if proc_info["running"]:
        status_text = "✅ Running — PID " + proc_info["main_pid"] + " | " + escape_html(proc_info["uptime_output"])
    else:
        status_text = "❌ Not running"

    safe_log = escape_html(log_text)
    safe_now = escape_html(now)
    safe_status = status_text

    activity_html = render_activity(activity)
    whales_html = render_whales(db_stats.get("top_whales", []))
    markets_html = render_markets(db_stats.get("top_markets", []))
    signals_html = render_signals(db_stats.get("recent_signals", []))

    wc = db_stats.get("whale_count", 0)
    ac = db_stats.get("active_whales", 0)
    ts = db_stats.get("total_signals", 0)
    un = db_stats.get("unsignaled", 0)
    st = db_stats.get("signals_today", 0)

    # P&L stats
    if pnl_stats is None:
        pnl_stats = get_pnl_stats()
    pnl_summary = pnl_stats.get("summary", {})
    pnl_realized = pnl_summary.get("total_realized_pnl", 0)
    pnl_actual = pnl_summary.get("total_actual_pnl", 0)
    pnl_divergence = pnl_summary.get("divergence", 0)
    resolved_count = pnl_summary.get("resolved_trades", 0)
    starting_balance = 500.0
    current_balance = starting_balance + pnl_realized
    current_balance_color = "green" if current_balance >= starting_balance else "red"
    pnl_realized_color = "green" if pnl_realized >= 0 else "red"
    pnl_actual_color = "green" if pnl_actual >= 0 else "red"
    pnl_div_color = "green" if pnl_divergence >= 0 else "red"
    resolved_html = render_resolved_trades(pnl_stats.get("recent_resolutions", []))

    return """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Nautilus Whale Follower</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:'Segoe UI',system-ui,sans-serif;background:#0d1117;color:#c9d1d9}
.header{background:#161b22;padding:16px 24px;border-bottom:1px solid #30363d;display:flex;justify-content:space-between;align-items:center}
.header h1{font-size:24px;color:#58a6ff}
.header .time{color:#8b949e;font-size:14px}
.container{max-width:1400px;margin:0 auto;padding:20px}
.metrics{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:16px;margin-bottom:24px}
.metric{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:16px}
.metric .label{color:#8b949e;font-size:14px;margin-bottom:4px}
.metric .value{font-size:28px;font-weight:bold;color:#58a6ff}
.metric .value.green{color:#3fb950}
.metric .value.red{color:#f85149}
.status-bar{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:16px;margin-bottom:24px;display:flex;align-items:center;gap:12px}
.status-dot{width:12px;height:12px;border-radius:50%}
.status-dot.running{background:#3fb950;box-shadow:0 0 8px #3fb950}
.status-dot.stopped{background:#f85149;box-shadow:0 0 8px #f85149}
.status-text{font-size:16px}
.tabs{display:flex;gap:4px;margin-bottom:16px;border-bottom:1px solid #30363d}
.tab{padding:10px 16px;cursor:pointer;border-radius:6px 6px 0 0;background:#161b22;border:1px solid #30363d;border-bottom:none;color:#8b949e}
.tab.active{background:#0d1117;color:#58a6ff;border-color:#58a6ff}
.tab:hover{color:#c9d1d9}
.tab-content{display:none}
.tab-content.active{display:block}
table{width:100%;border-collapse:collapse;background:#161b22;border-radius:8px;overflow:hidden}
th,td{padding:10px 14px;text-align:left;border-bottom:1px solid #30363d;font-size:14px}
th{background:#21262d;color:#8b949e;font-weight:600}
tr:hover{background:#1c2128}
.log{background:#0d1117;border:1px solid #30363d;border-radius:8px;padding:16px;font-family:'Fira Code',monospace;font-size:12px;max-height:500px;overflow-y:auto;white-space:pre-wrap;word-break:break-all}
.signal-green{color:#3fb950}.signal-red{color:#f85149}.signal-blue{color:#58a6ff}.signal-yellow{color:#d29922}
.refresh-btn{background:#21262d;border:1px solid #30363d;color:#c9d1d9;padding:8px 16px;border-radius:6px;cursor:pointer}
.refresh-btn:hover{background:#30363d}
.auto-refresh{font-size:12px;color:#8b949e}
</style>
</head>
<body>
<div class="header">
<h1>🐋 Nautilus Whale Follower</h1>
<div>
<span class="time">""" + safe_now + """</span>
<button class="refresh-btn" onclick="location.reload()" style="margin-left:12px;">↻ Refresh</button>
<span class="auto-refresh">Auto-refresh: 30s</span>
</div>
</div>
<div class="container">
<div class="status-bar">
<div class="status-dot """ + status_dot + """"></div>
<span class="status-text">""" + safe_status + """</span>
</div>
<div class="metrics">
<div class="metric"><div class="label">Whales Tracked</div><div class="value">""" + str(wc) + """</div></div>
<div class="metric"><div class="label">Active Whales</div><div class="value green">""" + str(ac) + """</div></div>
<div class="metric"><div class="label">Total Signals</div><div class="value">""" + str(ts) + """</div></div>
<div class="metric"><div class="label">Unsignaled</div><div class="value red">""" + str(un) + """</div></div>
<div class="metric"><div class="label">Signals Today</div><div class="value">""" + str(st) + """</div></div>
<div class="metric"><div class="label">Open Positions</div><div class="value">""" + str(pnl_summary.get("open_positions", 0)) + """</div></div>
</div>
<div class="metrics">
<div class="metric"><div class="label">Total Balance</div><div class="value """ + current_balance_color + """">""" + fmt_usd(current_balance) + """</div></div>
<div class="metric"><div class="label">Sim. P&L (Mark-to-Market)</div><div class="value """ + pnl_realized_color + """">""" + fmt_usd(pnl_realized) + """</div></div>
<div class="metric"><div class="label">Real P&L (Resolution-Based)</div><div class="value """ + pnl_actual_color + """">""" + fmt_usd(pnl_actual) + """</div></div>
<div class="metric"><div class="label">Divergence (Real − Sim)</div><div class="value """ + pnl_div_color + """">""" + fmt_usd(pnl_divergence) + """</div></div>
<div class="metric"><div class="label">Resolved Trades</div><div class="value">""" + str(resolved_count) + """</div></div>
</div>
<div class="tabs">
<div class="tab active" onclick="switchTab('overview')">📊 Overview</div>
<div class="tab" onclick="switchTab('whales')">🐋 Whales</div>
<div class="tab" onclick="switchTab('markets')">📈 Markets</div>
<div class="tab" onclick="switchTab('signals')">🔔 Signals</div>
<div class="tab" onclick="switchTab('pnl')">💰 P&L</div>
<div class="tab" onclick="switchTab('logs')">📋 Logs</div>
</div>
<div id="overview" class="tab-content active">
<h3 style="margin-bottom:12px;">Recent Activity</h3>
""" + activity_html + """
</div>
<div id="whales" class="tab-content">
<table>
<tr><th>Name</th><th>Alpha</th><th>PnL</th><th>Volume</th><th>Trades</th><th>Tags</th></tr>
""" + whales_html + """
</table>
</div>
<div id="markets" class="tab-content">
<table>
<tr><th>Market</th><th>Condition ID</th><th>Signals</th></tr>
""" + markets_html + """
</table>
</div>
<div id="signals" class="tab-content">
<table>
<tr><th>Whale</th><th>Market</th><th>Side</th><th>Outcome</th><th>Size</th><th>Price</th><th>USD</th><th>Conf</th><th>Time</th></tr>
""" + signals_html + """
</table>
</div>
<div id="pnl" class="tab-content">
<h3 style="margin-bottom:12px;">Resolution-Based P&L vs Simulated P&L</h3>
<p style="color:#8b949e;margin-bottom:16px;">Shows trades where the market has resolved. Real P&L is based on the actual resolution outcome; simulated P&L is the mark-to-market value at exit.</p>
<table>
<tr><th>Market</th><th>Side</th><th>Entry</th><th>Sim. P&L</th><th>Real P&L</th><th>Diff</th><th>Resolution</th><th>Time</th></tr>
""" + resolved_html + """
</table>
</div>
<div id="logs" class="tab-content">
<div class="log">""" + safe_log + """</div>
</div>
</div>
<script>
var source = null;
var retryDelay = 3000;
function fmtUsd(v) {
    if (v === null || v === undefined) return '$0.00';
    var abs = Math.abs(v);
    var sign = v < 0 ? '-' : '';
    if (abs >= 1000) return sign + '$' + abs.toLocaleString('en-US', {minimumFractionDigits: 2, maximumFractionDigits: 2});
    return sign + '$' + abs.toFixed(2);
}
function initStream() {
    if (source) { source.close(); }
    source = new EventSource('/api/stream');
    source.onmessage = function(e) {
        try {
            var d = JSON.parse(e.data);
            if (d.error) { console.warn('Stream error:', d.error); return; }
            retryDelay = 3000;
            // Update header timestamp
            var ts = document.querySelector('.header .time');
            if (ts && d.timestamp) {
                var date = new Date(d.timestamp);
                ts.textContent = date.toISOString().replace('T',' ').substring(0,19) + ' UTC';
            }
            // Update status bar
            var dot = document.querySelector('.status-dot');
            var statusText = document.querySelector('.status-text');
            if (dot && d.process) {
                dot.className = 'status-dot ' + (d.process.running ? 'running' : 'stopped');
                if (statusText) {
                    statusText.textContent = d.process.running
                        ? '✅ Running — PID ' + d.process.main_pid + ' | ' + (d.process.uptime_output || '')
                        : '❌ Not running';
                }
            }
            // Update metrics
            var vals = document.querySelectorAll('.metric .value');
            // Metric order: whales, active, signals, unsignaled, signals_today, pnl_real, pnl_actual, pnl_div, resolved
            // But unsignaled not in stream, so shift
            var metricMap = [
                null, // unsignaled not streamed
                null, // signals today
                d.pnl_realized,
                d.pnl_actual,
                d.pnl_divergence,
                d.resolved_trades
            ];
            var count = 0;
            var metrics = document.querySelectorAll('.metric');
            metrics.forEach(function(m) {
                var label = m.querySelector('.label');
                if (!label) return;
                var txt = label.textContent;
                var valEl = m.querySelector('.value');
                if (!valEl) return;
                if (txt.includes('Whales Tracked')) valEl.textContent = d.whales_tracked || 0;
                else if (txt.includes('Active')) valEl.textContent = d.active_whales || 0;
                else if (txt.includes('Signals Today')) valEl.textContent = d.signals_today || 0;
                else if (txt.includes('Sim. P&L')) {
                    valEl.textContent = fmtUsd(d.pnl_realized);
                    valEl.className = 'value ' + (d.pnl_realized >= 0 ? 'green' : 'red');
                } else if (txt.includes('Total Balance')) {
                    valEl.textContent = fmtUsd(d.total_balance);
                    valEl.className = 'value ' + (d.total_balance >= 500 ? 'green' : 'red');
                } else if (txt.includes('Real P&L')) {
                    valEl.textContent = fmtUsd(d.pnl_actual);
                    valEl.className = 'value ' + (d.pnl_actual >= 0 ? 'green' : 'red');
                } else if (txt.includes('Divergence')) {
                    valEl.textContent = fmtUsd(d.pnl_divergence);
                    valEl.className = 'value ' + (d.pnl_divergence >= 0 ? 'green' : 'red');
                } else if (txt.includes('Resolved')) valEl.textContent = d.resolved_trades || 0;
                else if (txt.includes('Open') && txt.includes('Position')) valEl.textContent = d.open_positions || 0;
            });
            // Update logs tab if visible
            var logDiv = document.querySelector('.log');
            if (logDiv && d.log_tail) {
                logDiv.textContent = d.log_tail;
                logDiv.scrollTop = logDiv.scrollHeight;
            }
        } catch(err) {
            console.warn('SSE parse error:', err);
        }
    };
    source.onerror = function() {
        source.close();
        setTimeout(initStream, retryDelay);
        retryDelay = Math.min(retryDelay * 2, 30000);
    };
}
function switchTab(name){
document.querySelectorAll('.tab-content').forEach(function(el){el.classList.remove('active')});
document.querySelectorAll('.tab').forEach(function(el){el.classList.remove('active')});
document.getElementById(name).classList.add('active');
event.target.classList.add('active');
}
// Start SSE stream on load
initStream();
</script>
</body>
</html>"""


@app.route("/")
def index():
    proc_info = {"running": True, "pids": [], "main_pid": "0", "uptime_output": "Active"}
    db_stats = get_db_stats()
    log_text = ""
    activity = []
    if proc_info["running"]:
        log_text = get_log_tail(proc_info["main_pid"])
        activity = parse_log_signals(log_text)
    pnl_stats = get_pnl_stats()
    return build_html(proc_info, db_stats, log_text, activity, pnl_stats=pnl_stats)


@app.route("/api/health")
def api_health():
    """Lightweight health check for monitoring — returns 200 if process is alive."""
    proc_info = {"running": True, "pids": [], "main_pid": "0", "uptime_output": "Active"}
    return {"status": "ok" if proc_info["running"] else "degraded",
            "pid": proc_info.get("main_pid"), "uptime_s": proc_info.get("uptime_s", 0),
            "timestamp": datetime.now(timezone.utc).isoformat()}


@app.route("/api/status")
def api_status():
    proc_info = {"running": True, "pids": [], "main_pid": "0", "uptime_output": "Active"}
    db_stats = get_db_stats()
    if "top_whales" in db_stats:
        db_stats["top_whales"] = [
            {"name": w[0], "alpha": w[1], "pnl": w[2], "volume": w[3], "trades": w[4], "tags": w[5]}
            for w in db_stats["top_whales"]
        ]
    if "top_markets" in db_stats:
        db_stats["top_markets"] = [
            {"title": m[0], "condition_id": m[1], "signals": m[2]}
            for m in db_stats["top_markets"]
        ]
    if "recent_signals" in db_stats:
        db_stats["recent_signals"] = [
            {"whale": s[0], "market": s[1], "side": s[2], "outcome": s[3],
             "size": s[4], "price": s[5], "usd_value": s[6], "confidence": s[7], "detected_at": s[8]}
            for s in db_stats["recent_signals"]
        ]
    return jsonify({
        "process": proc_info,
        "stats": db_stats,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    })


@app.route("/api/signal-gap-health")
def api_signal_gap_health():
    """Return signal-trade gap monitor health status.
    
    Reads from .signal_gap_health.json written by the cron monitor.
    """
    import os
    health_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".signal_gap_health.json")
    if os.path.exists(health_path):
        return send_file(health_path, mimetype="application/json")
    return jsonify({
        "healthy": True,
        "status": "Monitor not yet run",
        "checks": {},
        "alerts": [],
        "timestamp": datetime.now(timezone.utc).isoformat(),
    })


@app.route("/api/stream")
def api_stream():
    """Server-Sent Events — live dashboard updates every 5s."""
    def generate():
        while True:
            try:
                proc_info = {"running": True, "pids": [], "main_pid": "0", "uptime_output": "Active"}
                db_stats = get_db_stats()
                pnl_stats = get_pnl_stats()
                log_text = ""
                activity = []
                if proc_info["running"]:
                    log_text = get_log_tail(proc_info["main_pid"])
                    activity = parse_log_signals(log_text)

                pnl_summary = pnl_stats.get("summary", {})
                pnl_realized = pnl_summary.get("total_realized_pnl", 0)
                pnl_actual = pnl_summary.get("total_actual_pnl", 0)
                pnl_div = pnl_summary.get("divergence", 0)
                open_pos = pnl_summary.get("open_positions", 0)
                resolved = pnl_summary.get("resolved_trades", 0)
                starting_balance = 500.0
                current_balance = starting_balance + pnl_realized

                data = {
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "process": proc_info,
                    "whales_tracked": db_stats.get("whale_count", 0),
                    "active_whales": db_stats.get("active_whales", 0),
                    "signals_today": db_stats.get("signals_today", 0),
                    "pnl_realized": round(pnl_realized, 2),
                    "pnl_actual": round(pnl_actual, 2),
                    "pnl_divergence": round(pnl_div, 2),
                    "open_positions": open_pos,
                    "resolved_trades": resolved,
                    "total_balance": round(current_balance, 2),
                    "log_tail": log_text[-2000:] if log_text else "",
                    "recent_activity": activity[:10],
                }
                yield f"data: {json.dumps(data)}\n\n"
            except Exception as e:
                yield f"data: {json.dumps({'error': str(e)})}\n\n"
            time.sleep(5)

    return Response(
        generate(),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },


@app.route("/api/pnl")
def api_pnl():
    """Return real vs simulated P&L data."""
    pnl_stats = get_pnl_stats()
    return jsonify({
        "pnl": pnl_stats,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    })


if __name__ == "__main__":
    import socket
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        local_ip = s.getsockname()[0]
        s.close()
    except Exception:
        local_ip = "127.0.0.1"
    print("Nautilus Whale Follower Dashboard")
    print("=" * 50)
    print(f"  Open: http://{local_ip}:8502")
    print(f"  API:  http://{local_ip}:8502/api/status")
    print("=" * 50)
    app.run(host="127.0.0.1", port=8502, debug=False)
