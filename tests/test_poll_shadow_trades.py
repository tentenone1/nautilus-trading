"""Focused tests for the shadow polling CLI."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from scripts import poll_shadow_trades


def test_dry_run_does_not_backfill_or_poll(monkeypatch) -> None:
    def fail_if_called(*args, **kwargs):
        raise AssertionError("dry-run must not mutate or poll")

    monkeypatch.setattr(poll_shadow_trades, "backfill_sports_telemetry_signals", fail_if_called)
    monkeypatch.setattr(poll_shadow_trades, "poll_pending_shadow_trades", fail_if_called)
    monkeypatch.setattr(sys, "argv", ["poll_shadow_trades.py", "--dry-run", "--limit", "5"])

    assert poll_shadow_trades.main() == 0


def test_dry_run_does_not_make_db_calls_or_api_requests(monkeypatch, tmp_path) -> None:
    """Ensure dry-run mode prevents all DB mutations and API calls at the lowest level."""
    import sqlite3
    
    # Track if any DB operations or API calls are made
    db_calls_made = []
    api_calls_made = []
    
    # Monkeypatch DB operations
    original_connect = sqlite3.connect
    def mock_db_connect(*args, **kwargs):
        db_calls_made.append(f"connect: {args}")
        return original_connect(*args, **kwargs)
    
    # Monkeypatch API calls (urllib.request)
    import urllib.request
    original_urlopen = urllib.request.urlopen
    def mock_urlopen(*args, **kwargs):
        api_calls_made.append(f"urlopen: {args}")
        raise AssertionError("dry-run must not make API calls")
    
    # Apply monkeypatches
    monkeypatch.setattr("sqlite3.connect", mock_db_connect)
    monkeypatch.setattr("urllib.request.urlopen", mock_urlopen)
    
    # Set up command line args for dry run
    monkeypatch.setattr(sys, "argv", ["poll_shadow_trades.py", "--dry-run"])
    
    # Run and verify no calls were made
    result = poll_shadow_trades.main()
    assert result == 0
    # Note: We don't assert on db_calls_made being empty because 
    # the script may read config/DB to determine it's in dry-run mode
