"""Focused telemetry/performance integrity tests for SignalPipeline."""
import os
import sqlite3
import sys
from types import SimpleNamespace

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from strategies.signal_pipeline import SignalPipeline
import strategies.wf_db_ops as db_ops


class _FakeCursor:
    def __init__(self, row):
        self._row = row

    def fetchone(self):
        return self._row


class _FakeConn:
    def __init__(self, rows, queries):
        self._rows = list(rows)
        self._queries = queries

    def execute(self, sql, params=()):
        self._queries.append(sql)
        if "PRAGMA" in sql:
            return _FakeCursor(None)
        return _FakeCursor(self._rows.pop(0))

    def close(self):
        pass


def test_fade_eligibility_reads_canonical_perf(monkeypatch) -> None:
    queries = []
    rows = iter([(10, 2)])

    def fake_connect(_path):
        return _FakeConn([next(rows)], queries)

    monkeypatch.setattr(sqlite3, "connect", fake_connect)
    monkeypatch.setattr(db_ops, "ensure_canonical_perf_view", lambda db_path: None)
    pipeline = SignalPipeline(enable_edge_scorer=False, enable_sybil=False)

    assert pipeline._check_fade_eligibility("BadWhale", "general") is True
    assert any("FROM trades" in q for q in queries)


def test_fast_lane_reads_canonical_perf(monkeypatch) -> None:
    queries = []
    rows = iter([(100,), (20, 12)])

    def fake_connect(_path):
        return _FakeConn([next(rows)], queries)

    monkeypatch.setattr(sqlite3, "connect", fake_connect)
    monkeypatch.setattr(db_ops, "ensure_canonical_perf_view", lambda db_path: None)
    pipeline = SignalPipeline(enable_edge_scorer=False, enable_sybil=False)

    assert pipeline._check_fast_lane("GoodWhale", "general") is True
    assert any("FROM trades" in q for q in queries)
    assert not any("FROM canonical_perf" in q for q in queries)


def test_blacklist_fade_insufficient_data_sets_passed_fade(monkeypatch) -> None:
    pipeline = SignalPipeline(whale_tiering=None, enable_edge_scorer=False, enable_sybil=False)
    monkeypatch.setattr(pipeline, "_check_fade_eligibility", lambda *args, **kwargs: False)

    signal = SimpleNamespace(
        whale_name="TTEST2",
        market_category="general",
        market_title="Will this non-sports test pass?",
        condition_id="0xabc",
        side="BUY",
        confidence=0.9,
        edge_score=1.0,
    )

    result = pipeline.process(signal)

    # Pipeline sets reject_reason = "fade_insufficient_data" but continues
    # through remaining stages (final should_trade=True overwrites passed_fade_eligibility)
    assert result.reject_reason == "fade_insufficient_data"
    assert result.should_trade is True
