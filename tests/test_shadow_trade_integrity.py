"""Tests for shadow trade entry/size integrity fix.

Ensures future shadow_trades are not inserted with entry_price=0 or position_size_usd=0
for tradeable rows, and that the resolver does not fabricate $0 PnL for uncomputable rows.
"""

from __future__ import annotations

import os
import sqlite3
import sys
import tempfile
from unittest.mock import patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from strategies.wf_db_ops import ensure_shadow_trades_table, insert_decision_snapshot
from strategies import wf_shadow_ledger as ledger


def _make_db() -> str:
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    return path


def _cleanup_db(path: str) -> None:
    try:
        os.unlink(path)
    except FileNotFoundError:
        pass


# ── Test 1: insert_shadow_trade rejects zero entry_price ───────────────────

def test_insert_shadow_trade_rejects_zero_entry_price() -> None:
    """Future shadow_trades with entry_price=0 must be marked uncomputable."""
    db = _make_db()
    try:
        ensure_shadow_trades_table(db)
        # Patch db path so insert_shadow_trade writes to our temp DB
        with patch.object(ledger, "_get_db_path", lambda: db):
            row_id = ledger.insert_shadow_trade(
                signal_id="sig-1",
                snapshot_id=1,
                condition_id="0xcond",
                instrument_id="0xcond-tok.POLYMARKET",
                side="BUY",
                entry_price=0.0,
                position_size_usd=100.0,
                whale_name="TestWhale",
                whale_address="0x123",
                market_title="Test Market",
                category="general",
                edge_score=0.5,
                confidence=0.8,
                signal_type="COPY",
            )
        conn = sqlite3.connect(db)
        row = conn.execute(
            "SELECT entry_price, position_size_usd, block_reason, handler_step FROM shadow_trades WHERE id = ?",
            (row_id,),
        ).fetchone()
        conn.close()
        assert row is not None
        assert row[0] == 0.0
        assert row[1] == 100.0
        # Must be flagged as uncomputable, not left as a fake tradeable row
        assert row[2] == "uncomputable_zero_entry"
        assert row[3] == "uncomputable"
    finally:
        _cleanup_db(db)


# ── Test 2: insert_shadow_trade rejects zero position_size_usd ──────────────

def test_insert_shadow_trade_rejects_zero_size() -> None:
    """Future shadow_trades with position_size_usd=0 must be marked uncomputable."""
    db = _make_db()
    try:
        ensure_shadow_trades_table(db)
        with patch.object(ledger, "_get_db_path", lambda: db):
            row_id = ledger.insert_shadow_trade(
                signal_id="sig-2",
                snapshot_id=2,
                condition_id="0xcond",
                instrument_id="0xcond-tok.POLYMARKET",
                side="BUY",
                entry_price=0.50,
                position_size_usd=0.0,
                whale_name="TestWhale",
                whale_address="0x123",
                market_title="Test Market",
                category="general",
                edge_score=0.5,
                confidence=0.8,
                signal_type="COPY",
            )
        conn = sqlite3.connect(db)
        row = conn.execute(
            "SELECT entry_price, position_size_usd, block_reason, handler_step FROM shadow_trades WHERE id = ?",
            (row_id,),
        ).fetchone()
        conn.close()
        assert row is not None
        assert row[0] == 0.50
        assert row[1] == 0.0
        assert row[2] == "uncomputable_zero_size"
        assert row[3] == "uncomputable"
    finally:
        _cleanup_db(db)


# ── Test 3: insert_shadow_trade accepts valid entry/size ────────────────────

def test_insert_shadow_trade_accepts_valid_entry_and_size() -> None:
    """Valid entry_price and position_size_usd should be accepted normally."""
    db = _make_db()
    try:
        ensure_shadow_trades_table(db)
        with patch.object(ledger, "_get_db_path", lambda: db):
            row_id = ledger.insert_shadow_trade(
                signal_id="sig-3",
                snapshot_id=3,
                condition_id="0xcond",
                instrument_id="0xcond-tok.POLYMARKET",
                side="BUY",
                entry_price=0.50,
                position_size_usd=100.0,
                whale_name="TestWhale",
                whale_address="0x123",
                market_title="Test Market",
                category="general",
                edge_score=0.5,
                confidence=0.8,
                signal_type="COPY",
            )
        conn = sqlite3.connect(db)
        row = conn.execute(
            "SELECT entry_price, position_size_usd, block_reason, handler_step FROM shadow_trades WHERE id = ?",
            (row_id,),
        ).fetchone()
        conn.close()
        assert row is not None
        assert row[0] == 0.50
        assert row[1] == 100.0
        # No block_reason means it's a normal tradeable row
        assert row[2] is None or row[2] == ""
        assert row[3] is None or row[3] == ""
    finally:
        _cleanup_db(db)


# ── Test 4: resolver does not fabricate $0 PnL for uncomputable rows ────────

def test_resolver_skips_uncomputable_rows() -> None:
    """Rows with block_reason starting with 'uncomputable' must not get fake $0 PnL."""
    db = _make_db()
    try:
        ensure_shadow_trades_table(db)
        conn = sqlite3.connect(db)
        cursor = conn.execute(
            """
            INSERT INTO shadow_trades (
                signal_id, condition_id, side, entry_price, position_size_usd,
                whale_name, market_title, category, resolved, block_reason
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("sig-4", "0xcond", "BUY", 0.0, 0.0, "TestWhale", "Test", "general", 0, "uncomputable_zero_entry"),
        )
        row_id = cursor.lastrowid
        conn.commit()
        conn.close()

        with patch.object(ledger, "_get_db_path", lambda: db):
            with patch.object(
                ledger,
                "poll_market_resolution",
                lambda condition_id: {"resolved": True, "outcome": "YES"},
            ):
                result = ledger.resolve_shadow_trade(row_id, "0xcond")

        assert result is True  # market resolved
        conn = sqlite3.connect(db)
        row = conn.execute(
            "SELECT resolved, actual_pnl, hypothetical_pnl, won FROM shadow_trades WHERE id = ?",
            (row_id,),
        ).fetchone()
        conn.close()
        assert row[0] == 1  # resolved
        # Must NOT fabricate $0 PnL — leave as NULL or skip entirely
        assert row[1] is None  # actual_pnl
        assert row[2] is None  # hypothetical_pnl
        assert row[3] is None  # won
    finally:
        _cleanup_db(db)


# ── Test 5: resolver still computes PnL for valid rows ─────────────────────

def test_resolver_computes_pnl_for_valid_rows() -> None:
    """Valid rows must still get proper PnL computation."""
    db = _make_db()
    try:
        ensure_shadow_trades_table(db)
        conn = sqlite3.connect(db)
        cursor = conn.execute(
            """
            INSERT INTO shadow_trades (
                signal_id, condition_id, side, entry_price, position_size_usd,
                whale_name, market_title, category, resolved
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("sig-5", "0xcond", "BUY", 0.25, 100.0, "TestWhale", "Test", "general", 0),
        )
        row_id = cursor.lastrowid
        conn.commit()
        conn.close()

        with patch.object(ledger, "_get_db_path", lambda: db):
            with patch.object(
                ledger,
                "poll_market_resolution",
                lambda condition_id: {"resolved": True, "outcome": "YES"},
            ):
                result = ledger.resolve_shadow_trade(row_id, "0xcond")

        assert result is True
        conn = sqlite3.connect(db)
        row = conn.execute(
            "SELECT resolved, actual_pnl, hypothetical_pnl, won FROM shadow_trades WHERE id = ?",
            (row_id,),
        ).fetchone()
        conn.close()
        assert row[0] == 1
        assert row[1] == 300.0  # actual_pnl = 100 * (1/0.25 - 1) = 300
        assert row[2] == 300.0  # hypothetical_pnl
        assert row[3] == 1      # won
    finally:
        _cleanup_db(db)


# ── Test 6: step2_handler via insert_decision_snapshot writes valid sizes ──

def test_step2_handler_snapshot_writes_nonzero_entry_and_size() -> None:
    """When insert_decision_snapshot creates a SHADOW_TRADE, entry and size must be > 0."""
    db = _make_db()
    try:
        ensure_shadow_trades_table(db)
        # Also need decision_snapshots table
        from strategies.wf_db_ops import ensure_decision_snapshots_table
        ensure_decision_snapshots_table(db)

        with patch.object(ledger, "_get_db_path", lambda: db):
            success = insert_decision_snapshot(
                db_path=db,
                signal_id="sig-step2",
                source="known_whale",
                category="general",
                market_title="Test Market",
                condition_id="0xcond",
                whale_name="TestWhale",
                side="BUY",
                final_decision="SHADOW_TRADE",
                reject_reason="shadow_mode_block",
                position_size_usd=50.0,
                entry_price=0.60,
                metadata_json='{"handler_step2": true}',
            )

        assert success is True
        conn = sqlite3.connect(db)
        row = conn.execute(
            "SELECT entry_price, position_size_usd, block_reason, handler_step FROM shadow_trades WHERE signal_id = ?",
            ("sig-step2",),
        ).fetchone()
        conn.close()
        assert row is not None
        assert row[0] == 0.60
        assert row[1] == 50.0
        assert row[2] == "shadow_mode_block"
        assert row[3] == "step2_handler"
    finally:
        _cleanup_db(db)


# ── Test 7: FADE signals with zero size are marked observation-only ─────────

def test_fade_zero_size_marked_observation_only() -> None:
    """FADE signals without computed sizing must be marked observation-only."""
    db = _make_db()
    try:
        ensure_shadow_trades_table(db)
        with patch.object(ledger, "_get_db_path", lambda: db):
            row_id = ledger.insert_shadow_trade(
                signal_id="sig-fade",
                snapshot_id=6,
                condition_id="0xcond",
                instrument_id="0xcond-tok.POLYMARKET",
                side="SELL",
                entry_price=0.0,
                position_size_usd=0.0,
                whale_name="TestWhale",
                whale_address="0x123",
                market_title="Test Market",
                category="general",
                edge_score=0.5,
                confidence=0.8,
                signal_type="FADE",
            )
        conn = sqlite3.connect(db)
        row = conn.execute(
            "SELECT entry_price, position_size_usd, block_reason, handler_step, signal_type FROM shadow_trades WHERE id = ?",
            (row_id,),
        ).fetchone()
        conn.close()
        assert row is not None
        assert row[0] == 0.0
        assert row[1] == 0.0
        assert row[2] == "uncomputable_zero_entry"
        assert row[3] == "uncomputable"
        assert row[4] == "FADE"
    finally:
        _cleanup_db(db)


# ── Test 8: Sports telemetry backfill with zero entry is marked uncomputable ─

def test_sports_telemetry_backfill_zero_entry_marked_uncomputable() -> None:
    """Backfilled sports telemetry signals with entry_price=0 must be marked."""
    db = _make_db()
    try:
        ensure_shadow_trades_table(db)
        with patch.object(ledger, "_get_db_path", lambda: db):
            row_id = ledger.insert_shadow_trade(
                signal_id="sig-sports",
                snapshot_id=7,
                condition_id="0xcond",
                instrument_id=None,
                side="BUY",
                entry_price=0.0,
                position_size_usd=0.0,
                whale_name="TestWhale",
                whale_address="0x123",
                market_title="NBA Finals",
                category="sports",
                edge_score=0.5,
                confidence=0.8,
                signal_type="COPY",
                handler_step="step1_pipeline_sports_telemetry_mode",
                block_reason="sports_telemetry",
            )
        conn = sqlite3.connect(db)
        row = conn.execute(
            "SELECT entry_price, position_size_usd, block_reason, handler_step FROM shadow_trades WHERE id = ?",
            (row_id,),
        ).fetchone()
        meta_row = conn.execute(
            "SELECT metadata_json FROM shadow_trades WHERE id = ?",
            (row_id,),
        ).fetchone()
        conn.close()
        assert row is not None
        assert row[0] == 0.0
        assert row[1] == 0.0
        # Block reason is overwritten to uncomputable, but original preserved in metadata
        assert row[2] == "uncomputable_zero_entry"
        assert row[3] == "uncomputable"
        import json
        meta = json.loads(meta_row[0])
        assert meta.get("original_block_reason") == "sports_telemetry"
    finally:
        _cleanup_db(db)


# ── Test 9: step1_pipeline valid rows remain unchanged ──────────────────────

def test_step1_pipeline_valid_rows_unchanged() -> None:
    """step1_pipeline rows with valid entry/size should not get new block_reasons."""
    db = _make_db()
    try:
        ensure_shadow_trades_table(db)
        with patch.object(ledger, "_get_db_path", lambda: db):
            row_id = ledger.insert_shadow_trade(
                signal_id="sig-step1",
                snapshot_id=8,
                condition_id="0xcond",
                instrument_id="0xcond-tok.POLYMARKET",
                side="BUY",
                entry_price=0.45,
                position_size_usd=25.0,
                whale_name="TestWhale",
                whale_address="0x123",
                market_title="Test Market",
                category="general",
                edge_score=0.5,
                confidence=0.8,
                signal_type="COPY",
                handler_step="step1_pipeline",
            )
        conn = sqlite3.connect(db)
        row = conn.execute(
            "SELECT entry_price, position_size_usd, block_reason, handler_step FROM shadow_trades WHERE id = ?",
            (row_id,),
        ).fetchone()
        conn.close()
        assert row is not None
        assert row[0] == 0.45
        assert row[1] == 25.0
        assert row[2] is None or row[2] == ""
        assert row[3] == "step1_pipeline"
    finally:
        _cleanup_db(db)


# ── Test 10: Live trades table is untouched ─────────────────────────────────

def test_live_trades_table_untouched() -> None:
    """The fix must not write to or alter the trades (live) table."""
    db = _make_db()
    try:
        from strategies.wf_db_ops import _ensure_db_schema
        conn = sqlite3.connect(db)
        _ensure_db_schema(conn)
        # Count trades before shadow insert
        count_before = conn.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
        conn.close()
        # Insert shadow trade
        ensure_shadow_trades_table(db)
        with patch.object(ledger, "_get_db_path", lambda: db):
            ledger.insert_shadow_trade(
                signal_id="shadow-1",
                snapshot_id=9,
                condition_id="0xcond",
                instrument_id="0xcond-tok.POLYMARKET",
                side="BUY",
                entry_price=0.50,
                position_size_usd=50.0,
                whale_name="TestWhale",
                whale_address="0x123",
                market_title="Test Market",
                category="general",
                edge_score=0.5,
                confidence=0.8,
                signal_type="COPY",
            )
        conn = sqlite3.connect(db)
        count_after = conn.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
        conn.close()
        assert count_after == count_before  # unchanged
    finally:
        _cleanup_db(db)


# ── Test 11: wf_db_ops insert_shadow_trade with valid entry/size ───────────

def test_wf_db_ops_insert_valid_entry_size():
    db = _make_db()
    try:
        ensure_shadow_trades_table(db_path=db)
        from strategies.wf_db_ops import insert_shadow_trade
        ok = insert_shadow_trade(
            db_path=db,
            snapshot_id=1,
            condition_id="0xcond",
            instrument_id="0xcond-tok.POLYMARKET",
            side="BUY",
            entry_price=0.50,
            position_size_usd=100.0,
            whale_name="TestWhale",
            market_title="Test",
            category="general",
            edge_score=0.5,
            confidence=0.8,
            signal_type="COPY",
            entry_timestamp="2026-01-01T00:00:00+00:00",
            config_version="v6.6",
        )
        assert ok is True
        conn = sqlite3.connect(db)
        row = conn.execute(
            "SELECT entry_price, position_size_usd, block_reason, handler_step FROM shadow_trades WHERE snapshot_id = ?",
            (1,),
        ).fetchone()
        conn.close()
        assert row is not None
        assert row[0] == 0.50
        assert row[1] == 100.0
        # Valid rows should NOT be marked uncomputable
        assert row[2] == ""
        assert row[3] == ""
    finally:
        _cleanup_db(db)


# ── Test 12: wf_db_ops insert with entry_price=0 is marked uncomputable ─────

def test_wf_db_ops_insert_zero_entry_is_uncomputable():
    db = _make_db()
    try:
        ensure_shadow_trades_table(db_path=db)
        from strategies.wf_db_ops import insert_shadow_trade
        ok = insert_shadow_trade(
            db_path=db,
            snapshot_id=2,
            condition_id="0xcond",
            instrument_id="0xcond-tok.POLYMARKET",
            side="BUY",
            entry_price=0.0,
            position_size_usd=50.0,
            whale_name="TestWhale",
            market_title="Test",
            category="general",
            edge_score=0.5,
            confidence=0.8,
            signal_type="COPY",
            entry_timestamp="2026-01-01T00:00:00+00:00",
            config_version="v6.6",
        )
        assert ok is True
        conn = sqlite3.connect(db)
        row = conn.execute(
            "SELECT entry_price, position_size_usd, block_reason, handler_step, metadata_json FROM shadow_trades WHERE snapshot_id = ?",
            (2,),
        ).fetchone()
        conn.close()
        assert row is not None
        assert row[0] == 0.0
        assert row[1] == 50.0
        assert row[2] == "uncomputable_zero_entry"
        assert row[3] == "uncomputable"
    finally:
        _cleanup_db(db)


# ── Test 13: wf_db_ops insert with position_size_usd=0 is marked uncomputable ─

def test_wf_db_ops_insert_zero_size_is_uncomputable():
    db = _make_db()
    try:
        ensure_shadow_trades_table(db_path=db)
        from strategies.wf_db_ops import insert_shadow_trade
        ok = insert_shadow_trade(
            db_path=db,
            snapshot_id=3,
            condition_id="0xcond",
            instrument_id="0xcond-tok.POLYMARKET",
            side="BUY",
            entry_price=0.50,
            position_size_usd=0.0,
            whale_name="TestWhale",
            market_title="Test",
            category="general",
            edge_score=0.5,
            confidence=0.8,
            signal_type="COPY",
            entry_timestamp="2026-01-01T00:00:00+00:00",
            config_version="v6.6",
        )
        assert ok is True
        conn = sqlite3.connect(db)
        row = conn.execute(
            "SELECT entry_price, position_size_usd, block_reason, handler_step FROM shadow_trades WHERE snapshot_id = ?",
            (3,),
        ).fetchone()
        conn.close()
        assert row is not None
        assert row[0] == 0.50
        assert row[1] == 0.0
        assert row[2] == "uncomputable_zero_size"
        assert row[3] == "uncomputable"
    finally:
        _cleanup_db(db)


# ── Test 14: insert_decision_snapshot SHADOW_TRADE with zero entry/size ─────

def test_insert_decision_snapshot_shadow_trade_zero_entry_size_is_uncomputable():
    db = _make_db()
    try:
        from strategies.wf_db_ops import insert_decision_snapshot
        ensure_shadow_trades_table(db_path=db)
        # Patch _get_db_path so wf_shadow_ledger.insert_shadow_trade writes to temp DB
        with patch.object(ledger, "_get_db_path", lambda: db):
            ok = insert_decision_snapshot(
                db_path=db,
                signal_id="sig-zero",
                source="known_whale",
                category="general",
                market_title="Test Market",
                condition_id="0xcond",
                whale_name="TestWhale",
                side="BUY",
                final_decision="SHADOW_TRADE",
                reject_reason="shadow_mode_block",
                position_size_usd=0.0,
                entry_price=0.0,
                metadata_json='{"handler_step2": true}',
            )
        assert ok is True
        conn = sqlite3.connect(db)
        row = conn.execute(
            "SELECT entry_price, position_size_usd, block_reason, handler_step FROM shadow_trades WHERE signal_id = ?",
            ("sig-zero",),
        ).fetchone()
        conn.close()
        assert row is not None
        assert row[0] == 0.0
        assert row[1] == 0.0
        # Must be marked uncomputable, not left as a fake tradeable row
        assert row[2] == "uncomputable_zero_entry"
        assert row[3] == "uncomputable"
    finally:
        _cleanup_db(db)


# ── Test 15: wf_db_ops insert + resolver does not fabricate $0 PnL ──────────

def test_wf_db_ops_insert_uncomputable_resolver_skips():
    db = _make_db()
    try:
        ensure_shadow_trades_table(db_path=db)
        from strategies.wf_db_ops import insert_shadow_trade
        ok = insert_shadow_trade(
            db_path=db,
            snapshot_id=4,
            condition_id="0xcond",
            instrument_id="0xcond-tok.POLYMARKET",
            side="BUY",
            entry_price=0.0,
            position_size_usd=0.0,
            whale_name="TestWhale",
            market_title="Test",
            category="general",
            edge_score=0.5,
            confidence=0.8,
            signal_type="COPY",
            entry_timestamp="2026-01-01T00:00:00+00:00",
            config_version="v6.6",
        )
        assert ok is True
        conn = sqlite3.connect(db)
        row = conn.execute(
            "SELECT id FROM shadow_trades WHERE snapshot_id = ?", (4,)
        ).fetchone()
        shadow_id = row[0]
        conn.close()

        with patch.object(ledger, "_get_db_path", lambda: db):
            with patch.object(
                ledger,
                "poll_market_resolution",
                lambda condition_id: {"resolved": True, "outcome": "YES"},
            ):
                result = ledger.resolve_shadow_trade(shadow_id, "0xcond")

        assert result is True
        conn = sqlite3.connect(db)
        resolved = conn.execute(
            "SELECT resolved, actual_pnl, hypothetical_pnl, won FROM shadow_trades WHERE id = ?",
            (shadow_id,),
        ).fetchone()
        conn.close()
        assert resolved[0] == 1
        assert resolved[1] is None  # actual_pnl
        assert resolved[2] is None  # hypothetical_pnl
        assert resolved[3] is None  # won
    finally:
        _cleanup_db(db)


# ── Test 16: step2_handler-style path cannot create future tradeable zero row ─

def test_step2_handler_path_cannot_create_tradeable_zero_row():
    db = _make_db()
    try:
        from strategies.wf_db_ops import insert_decision_snapshot
        ensure_shadow_trades_table(db_path=db)
        # Patch the ledger's DB path so the nested insert_shadow_trade call writes to temp DB
        with patch.object(ledger, "_get_db_path", lambda: db):
            # Simulate step2_handler path: metadata has handler_step2 flag, sizing=0
            ok = insert_decision_snapshot(
                db_path=db,
                signal_id="sig-step2-zero",
                source="known_whale",
                category="general",
                market_title="Test Market",
                condition_id="0xcond",
                whale_name="TestWhale",
                side="BUY",
                final_decision="SHADOW_TRADE",
                reject_reason="shadow_mode_block",
                position_size_usd=0.0,
                entry_price=0.0,
                metadata_json='{"handler_step2": true, "would_have_traded": true}',
            )
        assert ok is True
        conn = sqlite3.connect(db)
        row = conn.execute(
            "SELECT entry_price, position_size_usd, block_reason, handler_step FROM shadow_trades WHERE signal_id = ?",
            ("sig-step2-zero",),
        ).fetchone()
        conn.close()
        assert row is not None
        assert row[0] == 0.0
        assert row[1] == 0.0
        # The integrity gate must override the step2_handler classification
        assert row[2] == "uncomputable_zero_entry"
        assert row[3] == "uncomputable"
    finally:
        _cleanup_db(db)


# ── Test 17: live trades count is 0 in test fixture ──────────────────────────

def test_live_trades_count_is_zero_in_fixture():
    db = _make_db()
    try:
        from strategies.wf_db_ops import _ensure_db_schema
        conn = sqlite3.connect(db)
        _ensure_db_schema(conn)
        count = conn.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
        conn.close()
        assert count == 0
    finally:
        _cleanup_db(db)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
