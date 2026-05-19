"""Fix whale_discovery.db schema — drop wrong tables, recreate correct schema."""
import sqlite3
import os

DB_PATH = "/home/elon-1/workspace/nautilus-trading/data/whale_discovery.db"

# Drop wrong tables
conn = sqlite3.connect(DB_PATH)
conn.execute("DROP TABLE IF EXISTS whales")
conn.execute("DROP TABLE IF EXISTS whale_signals")
conn.execute("DROP TABLE IF EXISTS health_metrics")
conn.execute("DROP TABLE IF EXISTS scan_log")
conn.execute("DROP TABLE IF EXISTS seen_positions")
conn.commit()
conn.close()
print("Old tables dropped.")

# Recreate with correct schema
from pipeline.db import init_db
init_db()
print("Schema re-initialized.")

# Verify
conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row
for table in ["whales", "whale_signals", "scan_log", "seen_positions"]:
    cols = [r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()]
    print(f"{table}: {cols}")
conn.close()

# Smoke test the queries that were failing
from pipeline.db import get_stats, get_unsignaled_signals, get_top_whales
print("\nSmoke tests:")
print("get_stats():", get_stats())
print("get_unsignaled_signals():", get_unsignaled_signals())
print("get_top_whales():", get_top_whales())
print("\nAll checks passed.")
