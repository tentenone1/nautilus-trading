"""Tests for autoresearch_signal_bridge.py TTL and state expiry logic + CLI integration.

Ensures:
- Metadata keys preserved
- Recommendation keys older than TTL expire
- Recent recommendation keys retained
- Non-numeric state values retained
- Dry-run performs no writes
- Execute mode removes only expired recommendation keys
- Malformed state does not delete everything
- Would-requeue count is reported
- No args selects daemon/run_loop path
- --report does not write state
- --execute writes only expired recommendation keys
- Queue file untouched
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
import time

sys.path.insert(0, "/home/elon-1/workspace/nautilus-trading/scripts")
sys.path.insert(0, "/home/elon-1/workspace/nautilus-trading")

import autoresearch_signal_bridge as bridge


# ── Helpers ───────────────────────────────────────────────────────────────────


def _write_json(path: str, data: dict | list) -> None:
    with open(path, "w") as f:
        json.dump(data, f)


def _read_json(path: str):
    with open(path) as f:
        return json.load(f)


# ── Core TTL Logic Tests ───────────────────────────────────────────────────


def test_meta_keys_preserved():
    state = {
        "last_run": "2026-06-01T00:00:00",
        "whales_updated": 9,
        "snapshot": "data",
    }
    cleaned, expired = bridge.expire_old_recommendation_keys(state, ttl_secs=86400)
    assert cleaned["last_run"] == "2026-06-01T00:00:00"
    assert cleaned["whales_updated"] == 9
    assert cleaned["snapshot"] == "data"
    assert expired == 0


def test_rec_key_expired():
    now = time.time()
    old_ts = now - (10 * 86400)  # 10 days ago
    state = {
        "last_run": "ok",
        "0xabc|2026-05-01T00:00:00": old_ts,
    }
    cleaned, expired = bridge.expire_old_recommendation_keys(state, ttl_secs=7 * 86400)
    assert expired == 1
    assert "0xabc|2026-05-01T00:00:00" not in cleaned
    assert cleaned["last_run"] == "ok"


def test_rec_key_retained():
    now = time.time()
    recent_ts = now - (2 * 86400)  # 2 days ago
    state = {
        "0xabc|2026-06-07T00:00:00": recent_ts,
    }
    cleaned, expired = bridge.expire_old_recommendation_keys(state, ttl_secs=7 * 86400)
    assert expired == 0
    assert "0xabc|2026-06-07T00:00:00" in cleaned


def test_non_numeric_values_retained():
    state = {
        "0xabc|2026-05-01T00:00:00": "not-a-number",
        "last_run": "ok",
    }
    cleaned, expired = bridge.expire_old_recommendation_keys(state, ttl_secs=86400)
    assert expired == 0
    assert cleaned["0xabc|2026-05-01T00:00:00"] == "not-a-number"


def test_malformed_state_safe():
    state = {
        "last_run": "ok",
        "whales_updated": 9,
        "snapshot": "data",
        "garbage_key": "garbage_value",
        "plain_pipe|": 123,
    }
    cleaned, expired = bridge.expire_old_recommendation_keys(state, ttl_secs=86400)
    assert cleaned["last_run"] == "ok"
    assert cleaned["whales_updated"] == 9
    assert cleaned["snapshot"] == "data"
    assert cleaned["garbage_key"] == "garbage_value"
    assert cleaned["plain_pipe|"] == 123
    assert expired == 0


# ── Dry-Run / Report Tests ─────────────────────────────────────────────────


def test_dry_run_performs_no_writes():
    fd, path = tempfile.mkstemp(suffix=".json")
    os.close(fd)
    try:
        now = time.time()
        old_ts = now - (10 * 86400)
        state = {"0xabc|2026-05-01T00:00:00": old_ts, "last_run": "ok"}
        _write_json(path, state)

        before = _read_json(path)
        assert "0xabc|2026-05-01T00:00:00" in before

        report = bridge.report_ttl(state, ttl_secs=7 * 86400, recommendations=[])

        after = _read_json(path)
        assert "0xabc|2026-05-01T00:00:00" in after  # file unchanged
        assert report["rec_expired"] == 1
        assert report["rec_retained"] == 0
    finally:
        os.unlink(path)


def test_would_requeue_reported():
    now = time.time()
    old_ts = now - (10 * 86400)
    state = {
        "0xcond1|2026-05-01T00:00:00": old_ts,
    }
    recommendations = [
        {
            "decision": "BUY",
            "confidence": 0.72,
            "condition_id": "0xcond1",
            "timestamp": "2026-05-01T00:00:00",
            "entry_price": 0.04,
            "kelly_fraction": 0.08,
            "market": "Test Market",
            "reason": "test",
            "hold_hours": 24,
        }
    ]
    report = bridge.report_ttl(state, ttl_secs=7 * 86400, recommendations=recommendations)
    assert report["buy_total"] == 1
    assert report["would_requeue"] == 1


def test_would_not_requeue_when_recent():
    now = time.time()
    recent_ts = now - (2 * 86400)
    state = {
        "0xcond1|2026-06-07T00:00:00": recent_ts,
    }
    recommendations = [
        {
            "decision": "BUY",
            "confidence": 0.72,
            "condition_id": "0xcond1",
            "timestamp": "2026-06-07T00:00:00",
            "entry_price": 0.04,
            "kelly_fraction": 0.08,
            "market": "Test Market",
            "reason": "test",
            "hold_hours": 24,
        }
    ]
    report = bridge.report_ttl(state, ttl_secs=7 * 86400, recommendations=recommendations)
    assert report["buy_total"] == 1
    assert report["would_requeue"] == 0


# ── Execute Tests (with temp STATE_FILE) ──────────────────────────────────────


def test_execute_removes_only_expired():
    now = time.time()
    old_ts = now - (10 * 86400)
    recent_ts = now - (2 * 86400)
    state = {
        "last_run": "ok",
        "0xold|2026-05-01T00:00:00": old_ts,
        "0xrecent|2026-06-07T00:00:00": recent_ts,
    }

    fd, temp_state = tempfile.mkstemp(suffix="_state.json")
    os.close(fd)
    _write_json(temp_state, state)

    orig_state_file = bridge.STATE_FILE
    try:
        bridge.STATE_FILE = temp_state
        result = bridge.run_ttl_execute(7)
        assert result == 0

        after = _read_json(temp_state)
        assert "0xold|2026-05-01T00:00:00" not in after
        assert "0xrecent|2026-06-07T00:00:00" in after
        assert after["last_run"] == "ok"
    finally:
        bridge.STATE_FILE = orig_state_file
        os.unlink(temp_state)


def test_execute_does_not_clear_queue():
    # Ensure the execute TTL mode never touches the queue file
    fd_queue, temp_queue = tempfile.mkstemp(suffix="_queue.json")
    os.close(fd_queue)
    _write_json(temp_queue, [])

    orig_queue = bridge.QUEUE_FILE
    try:
        bridge.QUEUE_FILE = temp_queue
        state = {"last_run": "ok", "0xold|2026-05-01T00:00:00": time.time() - (10 * 86400)}

        fd_state, temp_state = tempfile.mkstemp(suffix="_state.json")
        os.close(fd_state)
        _write_json(temp_state, state)
        orig_state = bridge.STATE_FILE
        bridge.STATE_FILE = temp_state

        bridge.run_ttl_execute(7)

        queue_after = _read_json(temp_queue)
        assert queue_after == []  # queue NOT touched

        bridge.STATE_FILE = orig_state
        os.unlink(temp_state)
    finally:
        bridge.QUEUE_FILE = orig_queue
        os.unlink(temp_queue)


# ── CLI Integration Tests ────────────────────────────────────────────────────


def test_cli_report_mode():
    now = time.time()
    old_ts = now - (10 * 86400)
    state = {"last_run": "ok", "0xold|2026-05-01T00:00:00": old_ts}

    fd, temp_state = tempfile.mkstemp(suffix="_state.json")
    os.close(fd)
    _write_json(temp_state, state)

    orig_recs = bridge.RECS_FILE
    orig_state = bridge.STATE_FILE
    try:
        bridge.RECS_FILE = "/dev/null"
        bridge.STATE_FILE = temp_state
        rc = bridge.cli(["--report", "--ttl-days", "7"])
        assert rc == 0

        after = _read_json(temp_state)
        assert "0xold|2026-05-01T00:00:00" in after  # dry-run: not removed
    finally:
        bridge.RECS_FILE = orig_recs
        bridge.STATE_FILE = orig_state
        os.unlink(temp_state)


def test_cli_execute_mode():
    now = time.time()
    old_ts = now - (10 * 86400)
    state = {"last_run": "ok", "0xold|2026-05-01T00:00:00": old_ts}

    fd, temp_state = tempfile.mkstemp(suffix="_state.json")
    os.close(fd)
    _write_json(temp_state, state)

    orig_recs = bridge.RECS_FILE
    orig_state = bridge.STATE_FILE
    try:
        bridge.RECS_FILE = "/dev/null"
        bridge.STATE_FILE = temp_state
        rc = bridge.cli(["--execute", "--ttl-days", "7"])
        assert rc == 0

        after = _read_json(temp_state)
        assert "0xold|2026-05-01T00:00:00" not in after  # removed
        assert after["last_run"] == "ok"  # metadata preserved
    finally:
        bridge.RECS_FILE = orig_recs
        bridge.STATE_FILE = orig_state
        os.unlink(temp_state)


def test_cli_no_args_selects_daemon_path():
    # Verify that no-arg invocation does NOT enter TTL report mode.
    # The cli() function without --report/--execute should fall through to run_loop.
    # We can't actually run run_loop in tests (blocks forever), so we verify
    # by intercepting run_loop at module level.
    called = []

    original_run_loop = bridge.run_loop

    def _fake_run_loop():
        called.append("run_loop")

    try:
        bridge.run_loop = _fake_run_loop
        rc = bridge.cli([])
        assert rc == 0
        assert called == ["run_loop"], f"Expected run_loop called, got: {called}"
    finally:
        bridge.run_loop = original_run_loop


if __name__ == "__main__":
    import pytest
    pytest.main([__file__, "-v"])
