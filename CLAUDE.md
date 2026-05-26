# CLAUDE.md — Nautilus Trading (1700)

## Project Structure

```
nautilus-trading/
├── strategies/
│   ├── whale_follower.py    # Main strategy: signal processing, position management
│   ├── wf_signal_handler.py # Signal ingestion and routing
│   └── wf_db_ops.py         # Whale DB operations
├── components/
│   ├── paper_execution.py   # Sandbox order execution (Polymarket live data)
│   ├── resolution_poller.py # Polls CLOB for market settlements
│   └── position_reconciler.py # Paper vs live position alignment
├── config/
│   └── whale_tiers.json     # Whale tiering, Kelly sizing, position caps
├── research/
│   └── trades.db            # All trade records (.gitignore'd, backup separately)
├── scripts/
│   ├── autoresearch_signal_bridge.py  # Signal bridge to Nautilus
│   └── *.py                 # Utility scripts
├── dashboard.py             # Streamlit UI on :8502
├── run_paper.py            # Paper trading entry point
└── run_micro_live.py       # Micro-live trading (guarded by .guard/micro-live.ok)
```

## Key Constants (whale_follower.py)

| Constant | Value | Purpose |
|----------|-------|---------|
| `MAX_SANE_RETURN` | `2.0` | ±200% P&L cap — filters sandbox artifacts |
| `EXIT_TIMER_INTERVAL_SECS` | `30.0` | Position exit check interval |
| `MEMORY_PRESSURE_MB` | `2500` | Graceful shutdown threshold |
| `max_open_positions` | `50` | Config field, not hard constant |

## Running the Trader

```bash
# Paper trading
cd /home/elon-1/workspace/nautilus-trading
TRADING_MODE=paper PAPER_TRADING=true ./venv/bin/python run_paper.py >> logs/paper_trading.log 2>&1

# Micro-live (requires .guard/micro-live.ok)
TRADING_MODE=live PAPER_TRADING=false ./venv/bin/python run_micro_live.py
```

## Coding Standards

- All code >5 lines → use OpenCode or Claw, not direct editing
- Type hints required, f-string logging, max 400 lines per file
- Never delete Bitable records — set Status=Cancelled only
- Numeric fixes require live verification before committing

## Git

- Remote: `git@github.com:tentenone1/nautilus-trading.git`
- Commit production changes immediately
- DB and logs are .gitignore'd

## Context Tips for Claw

- `AGENTS.md` has full architecture documentation — reference it for context
- `research/trades.db` is SQLite — query with `sqlite3 research/trades.db`
- Whale intel DB: `config/whale_tiers.json` — tier thresholds, Kelly fractions
- Dashboard: Streamlit on `:8502` — check if running with `curl -s localhost:8502`
