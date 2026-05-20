#!/usr/bin/env python3
"""Unified 6h Batch — no_agent cron.
Combines: Bitable Backup + X Scraper + Wiki Promoter + Whale Discovery
Silent when healthy. Outputs only on issues.
"""
import subprocess
import sys
import json
from pathlib import Path
from datetime import datetime, timezone

WORKSPACE = Path("/home/elon-1/workspace/nautilus-trading")
LOG_PREFIX = f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}]"
alerts = []

def run(script_name, args=None, cwd=None, timeout=120):
    """Run a script and return (ok, output)."""
    workdir = cwd or WORKSPACE
    script_path = WORKSPACE / script_name
    if not script_path.exists():
        return False, f"{script_name}: not found (skipped)"
    cmd = [sys.executable, str(script_path)] + (args or [])
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, cwd=str(workdir))
        out = r.stdout.strip() + r.stderr.strip()
        return r.returncode == 0, out[:300]
    except subprocess.TimeoutExpired:
        return False, f"{script_name}: timeout"
    except Exception as e:
        return False, f"{script_name}: {e}"

def run_sh(script_path, timeout=120):
    """Run a shell script."""
    try:
        r = subprocess.run(["bash", str(script_path)], capture_output=True, text=True, timeout=timeout)
        return r.returncode == 0, r.stdout.strip()[:300], r.stderr.strip()[:300]
    except subprocess.TimeoutExpired:
        return False, "timeout", ""
    except Exception as e:
        return False, str(e), ""

# ── 1. Bitable Backup ─────────────────────────────────────────────────
ok, out = run("research/bitable_backup.py")
if not ok and "not found (skipped)" not in out:
    alerts.append(f"⚠️ Bitable backup: {out}")
# Silent on success or if script doesn't exist yet

# ── 2. X Per-Account Scraper ──────────────────────────────────────────
ok, out, _ = run_sh(Path(WORKSPACE / "research" / "x-per-account-scraper.sh"), timeout=300)
if not ok and "not found" not in out:
    alerts.append(f"⚠️ X scraper: {out}")
# Silent on success or if script doesn't exist yet

# ── 3. Wiki Promoter ──────────────────────────────────────────────────
ok, out = run("wiki/wiki_promoter.py", timeout=60)
if not ok and "not found (skipped)" not in out:
    alerts.append(f"⚠️ Wiki promoter: {out}")
# Silent on success or if script doesn't exist yet

# ── 4. Whale Discovery Pipeline ────────────────────────────────────────
ok, out = run("research/whale_discovery.py", timeout=300)
if not ok and "not found (skipped)" not in out:
    alerts.append(f"⚠️ Whale discovery: {out}")
# Silent on success

# ── Output ──────────────────────────────────────────────────────────────
if alerts:
    print(f"{LOG_PREFIX} 6h Batch Issues:\n\n" + "\n".join(f"- {a}" for a in alerts))
else:
    print(f"{LOG_PREFIX} ✅ 6h batch complete — all systems nominal")
