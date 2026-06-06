"""
Focused behavioral tests for strategies.wf_category_action::get_category_action().

Observed-behavior only. No assertions about desired behavior.
Uses monkeypatching and temp files to avoid production data dependencies.
"""

import json
import os
import sqlite3
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from strategies import wf_category_action as wfca


# ── helpers ──────────────────────────────────────────────────────────────────

def _clear_all_caches():
    """Clear all module-level caches and mutable state for test isolation."""
    wfca.invalidate_cache()
    wfca._inactive_cache.clear()
    wfca._inactive_cache_time = 0.0


def _make_classifications_json(data: dict, tmpdir: str) -> Path:
    """Write a classifications JSON file and return its path."""
    path = Path(tmpdir) / "whale_category_classifications.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data))
    return path


def _make_discovery_db(tmpdir: str, rows: list) -> Path:
    """Create a discovery DB with pending_whales table and return its path."""
    path = Path(tmpdir) / "whale_discovery.db"
    conn = sqlite3.connect(str(path))
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pending_whales (
            whale_name TEXT, status TEXT, observed_trades INTEGER,
            win_rate REAL, avg_pnl REAL, total_pnl REAL, updated_at TEXT
        )
    """)
    for row in rows:
        conn.execute(
            "INSERT INTO pending_whales VALUES (?, ?, ?, ?, ?, ?, ?)",
            row,
        )
    conn.commit()
    conn.close()
    return path


# ── 1. category-specific lookup ──────────────────────────────────────────────

class TestCategorySpecificLookup:
    """1. category-specific lookup returns expected action/confidence when data exists."""

    def test_follow_action_for_known_whale_and_category(self, monkeypatch, tmp_path):
        _clear_all_caches()
        json_path = _make_classifications_json({
            "WhaleA": {
                "categories": {
                    "crypto": {
                        "action": "FOLLOW",
                        "action_confidence": 0.85,
                        "total_trades": 42,
                        "win_rate": 0.72,
                        "avg_pnl": 12.5,
                        "total_pnl": 525.0,
                    }
                }
            }
        }, tmp_path)
        monkeypatch.setattr(wfca, "_CLASSIFICATIONS_PATH", json_path)

        result = wfca.get_category_action("WhaleA", "crypto")

        assert result["action"] == "FOLLOW"
        assert result["action_confidence"] == 0.85
        assert result["source"] == "category_specific"
        assert result["stats"]["total_trades"] == 42
        assert result["stats"]["win_rate"] == 0.72
        assert result["stats"]["avg_pnl"] == 12.5
        assert result["stats"]["total_pnl"] == 525.0

    def test_fade_action_for_known_whale_and_category(self, monkeypatch, tmp_path):
        _clear_all_caches()
        json_path = _make_classifications_json({
            "WhaleB": {
                "categories": {
                    "sports": {
                        "action": "FADE",
                        "action_confidence": 0.75,
                        "total_trades": 20,
                        "win_rate": 0.15,
                        "avg_pnl": -8.0,
                        "total_pnl": -160.0,
                    }
                }
            }
        }, tmp_path)
        monkeypatch.setattr(wfca, "_CLASSIFICATIONS_PATH", json_path)

        result = wfca.get_category_action("WhaleB", "sports")

        assert result["action"] == "FADE"
        assert result["action_confidence"] == 0.75
        assert result["source"] == "category_specific"

    def test_neutral_action_for_known_whale_and_category(self, monkeypatch, tmp_path):
        _clear_all_caches()
        json_path = _make_classifications_json({
            "WhaleC": {
                "categories": {
                    "general": {
                        "action": "NEUTRAL",
                        "action_confidence": 0.60,
                        "total_trades": 15,
                        "win_rate": 0.48,
                        "avg_pnl": 0.5,
                        "total_pnl": 7.5,
                    }
                }
            }
        }, tmp_path)
        monkeypatch.setattr(wfca, "_CLASSIFICATIONS_PATH", json_path)

        result = wfca.get_category_action("WhaleC", "general")

        assert result["action"] == "NEUTRAL"
        assert result["action_confidence"] == 0.60

    def test_different_category_for_same_whale(self, monkeypatch, tmp_path):
        _clear_all_caches()
        json_path = _make_classifications_json({
            "WhaleD": {
                "categories": {
                    "crypto": {"action": "FOLLOW", "action_confidence": 0.9},
                    "sports": {"action": "FADE", "action_confidence": 0.7},
                }
            }
        }, tmp_path)
        monkeypatch.setattr(wfca, "_CLASSIFICATIONS_PATH", json_path)

        crypto = wfca.get_category_action("WhaleD", "crypto")
        sports = wfca.get_category_action("WhaleD", "sports")

        assert crypto["action"] == "FOLLOW"
        assert sports["action"] == "FADE"


# ── 2. global fallback ───────────────────────────────────────────────────────

class TestGlobalFallback:
    """2. global fallback works when category-specific data is absent."""

    def test_global_fallback_when_category_missing(self, monkeypatch, tmp_path):
        _clear_all_caches()
        json_path = _make_classifications_json({
            "WhaleE": {
                "global": {
                    "global_action": "FOLLOW",
                    "global_action_confidence": 0.80,
                    "total_trades": 100,
                    "win_rate": 0.65,
                    "avg_pnl": 5.0,
                    "total_pnl": 500.0,
                }
            }
        }, tmp_path)
        monkeypatch.setattr(wfca, "_CLASSIFICATIONS_PATH", json_path)
        # Make whale active so activity gate doesn't override
        monkeypatch.setattr(wfca, "is_whale_active", lambda *a, **k: (True, "test_active"))

        result = wfca.get_category_action("WhaleE", "politics")

        assert result["action"] == "FOLLOW"
        assert result["action_confidence"] == 0.80
        assert result["source"] == "global_fallback"
        assert result["stats"]["total_trades"] == 100

    def test_global_fallback_inactive_whale_returns_inactive(self, monkeypatch, tmp_path):
        """
        OBSERVED: When a whale has global data but is_whale_active returns False,
        the action is overridden to "INACTIVE" with confidence 0.05.
        "INACTIVE" is not documented in the function docstring's action enum.
        """
        _clear_all_caches()
        json_path = _make_classifications_json({
            "GhostWhale": {
                "global": {
                    "global_action": "FOLLOW",
                    "global_action_confidence": 0.90,
                    "total_trades": 50,
                }
            }
        }, tmp_path)
        monkeypatch.setattr(wfca, "_CLASSIFICATIONS_PATH", json_path)
        monkeypatch.setattr(wfca, "is_whale_active", lambda *a, **k: (False, "ghost_test"))

        result = wfca.get_category_action("GhostWhale", "crypto")

        assert result["action"] == "INACTIVE"
        assert result["action_confidence"] == 0.05
        assert result["source"] == "ghost_inactive"

    def test_global_fallback_empty_global_returns_insufficient(self, monkeypatch, tmp_path):
        """
        OBSERVED: If whale is in JSON but has no categories and empty global dict,
        the code falls through to pending check, then to default INSUFFICIENT_DATA.
        """
        _clear_all_caches()
        json_path = _make_classifications_json({
            "WhaleF": {}
        }, tmp_path)
        monkeypatch.setattr(wfca, "_CLASSIFICATIONS_PATH", json_path)
        monkeypatch.setattr(wfca, "is_whale_active", lambda *a, **k: (True, "test"))

        result = wfca.get_category_action("WhaleF", "crypto")

        assert result["action"] == "INSUFFICIENT_DATA"
        assert result["source"] == "default"


# ── 3. pending/probation fallback ────────────────────────────────────────────

class TestPendingProbationFallback:
    """3. pending/probation fallback works."""

    def test_probation_whale_returns_insufficient_with_scaled_confidence(
        self, monkeypatch, tmp_path
    ):
        _clear_all_caches()
        json_path = _make_classifications_json({}, tmp_path)
        db_path = _make_discovery_db(tmp_path, [
            ("ProbationWhale", "probation", 5, 0.3, 1.5, 7.5, "2026-06-01"),
        ])
        monkeypatch.setattr(wfca, "_CLASSIFICATIONS_PATH", json_path)
        monkeypatch.setattr(wfca, "_DISCOVERY_DB_PATH", db_path)

        result = wfca.get_category_action("ProbationWhale", "general")

        assert result["action"] == "INSUFFICIENT_DATA"
        assert result["source"] == "probation"
        # 5 trades / 10 minimum * 0.5 = 0.25
        assert result["action_confidence"] == 0.25
        assert result["stats"]["total_trades"] == 5

    def test_probation_at_threshold_returns_max_confidence(self, monkeypatch, tmp_path):
        _clear_all_caches()
        json_path = _make_classifications_json({}, tmp_path)
        db_path = _make_discovery_db(tmp_path, [
            ("ReadyWhale", "probation", 10, 0.5, 2.0, 20.0, "2026-06-01"),
        ])
        monkeypatch.setattr(wfca, "_CLASSIFICATIONS_PATH", json_path)
        monkeypatch.setattr(wfca, "_DISCOVERY_DB_PATH", db_path)

        result = wfca.get_category_action("ReadyWhale", "general")

        assert result["action"] == "INSUFFICIENT_DATA"
        assert result["source"] == "probation"
        # 10 trades / 10 minimum, capped at 1.0, * 0.5 = 0.5
        assert result["action_confidence"] == 0.5

    def test_probation_zero_trades_returns_zero_confidence(self, monkeypatch, tmp_path):
        _clear_all_caches()
        json_path = _make_classifications_json({}, tmp_path)
        db_path = _make_discovery_db(tmp_path, [
            ("NewWhale", "probation", 0, None, None, None, "2026-06-01"),
        ])
        monkeypatch.setattr(wfca, "_CLASSIFICATIONS_PATH", json_path)
        monkeypatch.setattr(wfca, "_DISCOVERY_DB_PATH", db_path)

        result = wfca.get_category_action("NewWhale", "general")

        assert result["action"] == "INSUFFICIENT_DATA"
        assert result["action_confidence"] == 0.0
        assert result["stats"]["total_trades"] == 0


# ── 4. unknown whale ─────────────────────────────────────────────────────────

class TestUnknownWhale:
    """4. unknown whale/category returns INSUFFICIENT_DATA."""

    def test_unknown_whale_returns_default(self, monkeypatch, tmp_path):
        _clear_all_caches()
        json_path = _make_classifications_json({"OtherWhale": {}}, tmp_path)
        monkeypatch.setattr(wfca, "_CLASSIFICATIONS_PATH", json_path)

        result = wfca.get_category_action("UnknownWhale", "crypto")

        assert result["action"] == "INSUFFICIENT_DATA"
        assert result["action_confidence"] == 0.3
        assert result["source"] == "default"
        assert result["stats"] == {}

    def test_empty_whale_name_returns_default(self, monkeypatch, tmp_path):
        _clear_all_caches()
        json_path = _make_classifications_json({"OtherWhale": {}}, tmp_path)
        monkeypatch.setattr(wfca, "_CLASSIFICATIONS_PATH", json_path)

        result = wfca.get_category_action("", "crypto")

        assert result["action"] == "INSUFFICIENT_DATA"
        assert result["action_confidence"] == 0.0
        assert result["source"] == "default"


# ── 5. missing data file ─────────────────────────────────────────────────────

class TestMissingDataFile:
    """5. missing or empty data file returns INSUFFICIENT_DATA instead of crashing."""

    def test_missing_json_returns_default(self, monkeypatch, tmp_path):
        _clear_all_caches()
        nonexistent = tmp_path / "does_not_exist.json"
        monkeypatch.setattr(wfca, "_CLASSIFICATIONS_PATH", nonexistent)

        result = wfca.get_category_action("AnyWhale", "crypto")

        assert result["action"] == "INSUFFICIENT_DATA"
        assert result["source"] == "default"


# ── 6. malformed data ──────────────────────────────────────────────────────────

class TestMalformedData:
    """6. malformed data now falls back safely after hardening patch."""

    def test_malformed_json_returns_insufficient_data(self, monkeypatch, tmp_path):
        """
        HARDENED: _load_classifications() now catches JSONDecodeError and
        returns empty dict. get_category_action() then falls through to the
        default INSUFFICIENT_DATA return instead of raising.
        """
        _clear_all_caches()
        bad_path = tmp_path / "bad.json"
        bad_path.write_text("{not valid json")
        monkeypatch.setattr(wfca, "_CLASSIFICATIONS_PATH", bad_path)

        result = wfca.get_category_action("AnyWhale", "crypto")
        assert result["action"] == "INSUFFICIENT_DATA"
        assert result["source"] == "default"

    def test_unreadable_file_returns_insufficient_data(self, monkeypatch, tmp_path):
        """
        HARDENED: OSError on read (permissions, etc.) is caught and falls
        back to INSUFFICIENT_DATA.
        """
        _clear_all_caches()
        bad_path = tmp_path / "unreadable.json"
        bad_path.write_text("{}")
        bad_path.chmod(0o000)
        monkeypatch.setattr(wfca, "_CLASSIFICATIONS_PATH", bad_path)
        try:
            result = wfca.get_category_action("AnyWhale", "crypto")
            assert result["action"] == "INSUFFICIENT_DATA"
            assert result["source"] == "default"
        finally:
            bad_path.chmod(0o644)


# ── 7. confidence value stability ────────────────────────────────────────────

class TestConfidenceValues:
    """7. confidence value is present and stable according to current behavior."""

    def test_default_confidence_for_unknown_is_0_3(self, monkeypatch, tmp_path):
        _clear_all_caches()
        json_path = _make_classifications_json({}, tmp_path)
        monkeypatch.setattr(wfca, "_CLASSIFICATIONS_PATH", json_path)

        result = wfca.get_category_action("NoOne", "crypto")
        assert result["action_confidence"] == 0.3

    def test_category_specific_confidence_from_json(self, monkeypatch, tmp_path):
        _clear_all_caches()
        json_path = _make_classifications_json({
            "WhaleG": {
                "categories": {
                    "sports": {"action": "FOLLOW", "action_confidence": 0.92}
                }
            }
        }, tmp_path)
        monkeypatch.setattr(wfca, "_CLASSIFICATIONS_PATH", json_path)

        result = wfca.get_category_action("WhaleG", "sports")
        assert result["action_confidence"] == 0.92

    def test_global_fallback_uses_default_0_3_when_not_set(self, monkeypatch, tmp_path):
        _clear_all_caches()
        json_path = _make_classifications_json({
            "WhaleH": {
                "global": {
                    "global_action": "FADE",
                    # no global_action_confidence field
                }
            }
        }, tmp_path)
        monkeypatch.setattr(wfca, "_CLASSIFICATIONS_PATH", json_path)
        monkeypatch.setattr(wfca, "is_whale_active", lambda *a, **k: (True, "test"))

        result = wfca.get_category_action("WhaleH", "crypto")
        # Default fallback for missing confidence field
        assert result["action_confidence"] == 0.3


# ── 8. result shape stability ────────────────────────────────────────────────

class TestResultShape:
    """8. result shape/keys are stable."""

    def test_all_expected_keys_present_for_category_specific(self, monkeypatch, tmp_path):
        _clear_all_caches()
        json_path = _make_classifications_json({
            "WhaleI": {
                "categories": {
                    "crypto": {"action": "FOLLOW", "action_confidence": 0.8}
                }
            }
        }, tmp_path)
        monkeypatch.setattr(wfca, "_CLASSIFICATIONS_PATH", json_path)

        result = wfca.get_category_action("WhaleI", "crypto")

        assert set(result.keys()) == {"action", "action_confidence", "source", "stats"}
        assert set(result["stats"].keys()) == {"total_trades", "win_rate", "avg_pnl", "total_pnl"}

    def test_all_expected_keys_present_for_unknown(self, monkeypatch, tmp_path):
        _clear_all_caches()
        json_path = _make_classifications_json({}, tmp_path)
        monkeypatch.setattr(wfca, "_CLASSIFICATIONS_PATH", json_path)

        result = wfca.get_category_action("NoOne", "crypto")

        assert set(result.keys()) == {"action", "action_confidence", "source", "stats"}
        assert result["stats"] == {}


# ── 9. no None for action ────────────────────────────────────────────────────

class TestNullAction:
    """9. null/blank/missing actions now return INSUFFICIENT_DATA after hardening patch."""

    def test_null_action_returns_insufficient_data(self, monkeypatch, tmp_path):
        """
        HARDENED: _sanitize_action() converts None to "INSUFFICIENT_DATA".
        Previously this returned None directly.
        """
        _clear_all_caches()
        json_path = _make_classifications_json({
            "WhaleJ": {
                "categories": {
                    "crypto": {"action": None, "action_confidence": 0.5}
                }
            }
        }, tmp_path)
        monkeypatch.setattr(wfca, "_CLASSIFICATIONS_PATH", json_path)

        result = wfca.get_category_action("WhaleJ", "crypto")
        assert result["action"] == "INSUFFICIENT_DATA"
        assert isinstance(result["action"], str)

    def test_blank_action_returns_insufficient_data(self, monkeypatch, tmp_path):
        """
        HARDENED: _sanitize_action() converts blank/whitespace strings to
        "INSUFFICIENT_DATA".
        """
        _clear_all_caches()
        json_path = _make_classifications_json({
            "WhaleJ2": {
                "categories": {
                    "crypto": {"action": "   ", "action_confidence": 0.5}
                }
            }
        }, tmp_path)
        monkeypatch.setattr(wfca, "_CLASSIFICATIONS_PATH", json_path)

        result = wfca.get_category_action("WhaleJ2", "crypto")
        assert result["action"] == "INSUFFICIENT_DATA"

    def test_missing_action_returns_insufficient_data(self, monkeypatch, tmp_path):
        """
        HARDENED: Missing action key still uses .get() default, but now
        passes through _sanitize_action() for consistency.
        """
        _clear_all_caches()
        json_path = _make_classifications_json({
            "WhaleJ3": {
                "categories": {
                    "crypto": {"action_confidence": 0.5}
                }
            }
        }, tmp_path)
        monkeypatch.setattr(wfca, "_CLASSIFICATIONS_PATH", json_path)

        result = wfca.get_category_action("WhaleJ3", "crypto")
        assert result["action"] == "INSUFFICIENT_DATA"

    def test_normal_fallback_returns_string_not_none(self, monkeypatch, tmp_path):
        """
        SAFE BEHAVIOR: Under normal code paths (no nulls in JSON,
        missing keys), get_category_action() always returns a string action.
        Unknown whales, missing files, and empty globals all fall through
        to the explicit default return with action="INSUFFICIENT_DATA".
        """
        _clear_all_caches()
        json_path = _make_classifications_json({}, tmp_path)
        monkeypatch.setattr(wfca, "_CLASSIFICATIONS_PATH", json_path)

        result = wfca.get_category_action("X", "y")
        assert isinstance(result["action"], str)
        assert result["action"] == "INSUFFICIENT_DATA"

    def test_action_invariant_never_none(self, monkeypatch, tmp_path):
        """
        INVARIANT: For any classification JSON state (valid, malformed,
        missing, null action, blank action), get_category_action() must
        never return action=None.
        """
        # Case 1: null action
        _clear_all_caches()
        null_path = _make_classifications_json({
            "W1": {"categories": {"c": {"action": None}}}
        }, tmp_path)
        monkeypatch.setattr(wfca, "_CLASSIFICATIONS_PATH", null_path)
        assert wfca.get_category_action("W1", "c")["action"] is not None
        assert isinstance(wfca.get_category_action("W1", "c")["action"], str)

        # Case 2: blank action
        _clear_all_caches()
        blank_path = _make_classifications_json({
            "W2": {"categories": {"c": {"action": "   "}}}
        }, tmp_path)
        monkeypatch.setattr(wfca, "_CLASSIFICATIONS_PATH", blank_path)
        assert wfca.get_category_action("W2", "c")["action"] is not None
        assert isinstance(wfca.get_category_action("W2", "c")["action"], str)

        # Case 3: missing action key
        _clear_all_caches()
        missing_path = _make_classifications_json({
            "W3": {"categories": {"c": {"action_confidence": 0.5}}}
        }, tmp_path)
        monkeypatch.setattr(wfca, "_CLASSIFICATIONS_PATH", missing_path)
        assert wfca.get_category_action("W3", "c")["action"] is not None
        assert isinstance(wfca.get_category_action("W3", "c")["action"], str)

        # Case 4: malformed JSON
        _clear_all_caches()
        bad_path = tmp_path / "bad.json"
        bad_path.write_text("{not valid json")
        monkeypatch.setattr(wfca, "_CLASSIFICATIONS_PATH", bad_path)
        assert wfca.get_category_action("W4", "c")["action"] is not None
        assert isinstance(wfca.get_category_action("W4", "c")["action"], str)

        # Case 5: missing file
        _clear_all_caches()
        nofile = tmp_path / "missing.json"
        monkeypatch.setattr(wfca, "_CLASSIFICATIONS_PATH", nofile)
        assert wfca.get_category_action("W5", "c")["action"] is not None
        assert isinstance(wfca.get_category_action("W5", "c")["action"], str)

        # Case 6: valid action
        _clear_all_caches()
        valid_path = _make_classifications_json({
            "W6": {"categories": {"c": {"action": "FOLLOW"}}}
        }, tmp_path)
        monkeypatch.setattr(wfca, "_CLASSIFICATIONS_PATH", valid_path)
        assert wfca.get_category_action("W6", "c")["action"] is not None
        assert isinstance(wfca.get_category_action("W6", "c")["action"], str)


# ── Caplog logging verification ──────────────────────────────────────────────

class TestFallbackLogging:
    """Logging coverage: every hardening fallback must emit a warning."""

    def test_malformed_json_logs_load_failure(self, monkeypatch, tmp_path, caplog):
        import logging
        _clear_all_caches()
        bad_path = tmp_path / "bad.json"
        bad_path.write_text("{not valid json")
        monkeypatch.setattr(wfca, "_CLASSIFICATIONS_PATH", bad_path)

        with caplog.at_level(logging.WARNING, logger="strategies.wf_category_action"):
            result = wfca.get_category_action("AnyWhale", "crypto")

        assert result["action"] == "INSUFFICIENT_DATA"
        assert "classification_load_failed" in caplog.text

    def test_null_action_logs_invalid_action(self, monkeypatch, tmp_path, caplog):
        import logging
        _clear_all_caches()
        json_path = _make_classifications_json({
            "WhaleJ": {
                "categories": {
                    "crypto": {"action": None, "action_confidence": 0.5}
                }
            }
        }, tmp_path)
        monkeypatch.setattr(wfca, "_CLASSIFICATIONS_PATH", json_path)

        with caplog.at_level(logging.WARNING, logger="strategies.wf_category_action"):
            result = wfca.get_category_action("WhaleJ", "crypto")

        assert result["action"] == "INSUFFICIENT_DATA"
        assert "invalid_classification_action" in caplog.text
        assert "WhaleJ" in caplog.text

    def test_blank_action_logs_invalid_action(self, monkeypatch, tmp_path, caplog):
        import logging
        _clear_all_caches()
        json_path = _make_classifications_json({
            "WhaleJ2": {
                "categories": {
                    "crypto": {"action": "   ", "action_confidence": 0.5}
                }
            }
        }, tmp_path)
        monkeypatch.setattr(wfca, "_CLASSIFICATIONS_PATH", json_path)

        with caplog.at_level(logging.WARNING, logger="strategies.wf_category_action"):
            result = wfca.get_category_action("WhaleJ2", "crypto")

        assert result["action"] == "INSUFFICIENT_DATA"
        assert "invalid_classification_action" in caplog.text
        assert "'   '" in caplog.text or '"   "' in caplog.text

    def test_missing_category_action_key_logs_and_returns_insufficient_data(
        self, monkeypatch, tmp_path, caplog
    ):
        """
        HARDENED: Missing "action" key in a category-specific classification
        entry now logs a warning (via _sanitize_action receiving None) and
        returns "INSUFFICIENT_DATA".
        """
        import logging
        _clear_all_caches()
        json_path = _make_classifications_json({
            "WhaleJ3": {
                "categories": {
                    "crypto": {"action_confidence": 0.5}
                }
            }
        }, tmp_path)
        monkeypatch.setattr(wfca, "_CLASSIFICATIONS_PATH", json_path)

        with caplog.at_level(logging.WARNING, logger="strategies.wf_category_action"):
            result = wfca.get_category_action("WhaleJ3", "crypto")

        assert result["action"] == "INSUFFICIENT_DATA"
        assert "invalid_classification_action" in caplog.text
        assert "WhaleJ3" in caplog.text
        assert "crypto" in caplog.text

    def test_non_string_action_logs_invalid_action(self, monkeypatch, tmp_path, caplog):
        """
        HARDENED: Integer, list, or other non-string action values are
        caught by _sanitize_action() and logged as invalid.
        """
        import logging
        _clear_all_caches()
        json_path = _make_classifications_json({
            "WhaleJ4": {
                "categories": {
                    "crypto": {"action": 42, "action_confidence": 0.5}
                }
            }
        }, tmp_path)
        monkeypatch.setattr(wfca, "_CLASSIFICATIONS_PATH", json_path)

        with caplog.at_level(logging.WARNING, logger="strategies.wf_category_action"):
            result = wfca.get_category_action("WhaleJ4", "crypto")

        assert result["action"] == "INSUFFICIENT_DATA"
        assert "invalid_classification_action" in caplog.text
        assert "42" in caplog.text

    def test_missing_global_action_key_logs_and_returns_insufficient_data(
        self, monkeypatch, tmp_path, caplog
    ):
        """
        HARDENED: Missing "global_action" key in a global classification
        entry now logs a warning (via _sanitize_action receiving None) and
        returns "INSUFFICIENT_DATA".
        """
        import logging
        _clear_all_caches()
        json_path = _make_classifications_json({
            "WhaleG2": {
                "global": {
                    "global_action_confidence": 0.8,
                }
            }
        }, tmp_path)
        monkeypatch.setattr(wfca, "_CLASSIFICATIONS_PATH", json_path)
        monkeypatch.setattr(wfca, "is_whale_active", lambda *a, **k: (True, "test"))

        with caplog.at_level(logging.WARNING, logger="strategies.wf_category_action"):
            result = wfca.get_category_action("WhaleG2", "crypto")

        assert result["action"] == "INSUFFICIENT_DATA"
        assert result["source"] == "global_fallback"
        assert "invalid_classification_action" in caplog.text
        assert "WhaleG2" in caplog.text
        assert "global" in caplog.text

    def test_valid_action_does_not_log_fallback_warning(self, monkeypatch, tmp_path, caplog):
        import logging
        _clear_all_caches()
        json_path = _make_classifications_json({
            "GoodWhale": {
                "categories": {
                    "crypto": {"action": "FOLLOW", "action_confidence": 0.9}
                }
            }
        }, tmp_path)
        monkeypatch.setattr(wfca, "_CLASSIFICATIONS_PATH", json_path)

        with caplog.at_level(logging.WARNING, logger="strategies.wf_category_action"):
            result = wfca.get_category_action("GoodWhale", "crypto")

        assert result["action"] == "FOLLOW"
        assert "invalid_classification_action" not in caplog.text
        assert "classification_load_failed" not in caplog.text


# ── 10. deterministic repeated calls ────────────────────────────────────────

class TestDeterminism:
    """10. repeated calls with the same inputs are deterministic when data is unchanged."""

    def test_repeated_calls_return_identical_results(self, monkeypatch, tmp_path):
        _clear_all_caches()
        json_path = _make_classifications_json({
            "WhaleK": {
                "categories": {
                    "general": {"action": "FOLLOW", "action_confidence": 0.77}
                }
            }
        }, tmp_path)
        monkeypatch.setattr(wfca, "_CLASSIFICATIONS_PATH", json_path)

        first = wfca.get_category_action("WhaleK", "general")
        second = wfca.get_category_action("WhaleK", "general")
        third = wfca.get_category_action("WhaleK", "general")

        assert first == second == third

    def test_cache_hit_produces_same_result(self, monkeypatch, tmp_path):
        """
        OBSERVED: _load_classifications() caches JSON for 60 seconds.
        Repeated calls within TTL return the same result without re-reading file.
        """
        _clear_all_caches()
        json_path = _make_classifications_json({
            "WhaleL": {
                "categories": {
                    "crypto": {"action": "NEUTRAL", "action_confidence": 0.6}
                }
            }
        }, tmp_path)
        monkeypatch.setattr(wfca, "_CLASSIFICATIONS_PATH", json_path)

        r1 = wfca.get_category_action("WhaleL", "crypto")
        # Overwrite file with different data to prove cache is used
        json_path.write_text(json.dumps({
            "WhaleL": {
                "categories": {
                    "crypto": {"action": "FOLLOW", "action_confidence": 0.99}
                }
            }
        }))
        r2 = wfca.get_category_action("WhaleL", "crypto")

        assert r1 == r2  # cached result, not reloaded
        assert r2["action"] == "NEUTRAL"

    def test_cache_invalidation_reloads(self, monkeypatch, tmp_path):
        _clear_all_caches()
        json_path = _make_classifications_json({
            "WhaleM": {
                "categories": {
                    "crypto": {"action": "NEUTRAL", "action_confidence": 0.6}
                }
            }
        }, tmp_path)
        monkeypatch.setattr(wfca, "_CLASSIFICATIONS_PATH", json_path)

        r1 = wfca.get_category_action("WhaleM", "crypto")
        # Overwrite file
        json_path.write_text(json.dumps({
            "WhaleM": {
                "categories": {
                    "crypto": {"action": "FOLLOW", "action_confidence": 0.99}
                }
            }
        }))
        wfca.invalidate_cache()
        r2 = wfca.get_category_action("WhaleM", "crypto")

        assert r1 != r2
        assert r2["action"] == "FOLLOW"


# ── 11. classifier_path parameter behavior ───────────────────────────────────

class TestClassificationsPathParameter:
    """
    OBSERVED: get_category_action() accepts a `classifications_path` parameter,
    but _load_classifications() ignores it and always reads _CLASSIFICATIONS_PATH.
    The parameter only affects is_whale_active()'s discovery DB path (also broken).
    This is current behavior — the parameter is documented but non-functional
    for the primary JSON loading.
    """

    def test_classifications_path_parameter_is_ignored_for_json(self, monkeypatch, tmp_path):
        _clear_all_caches()
        real_path = _make_classifications_json({
            "WhaleN": {
                "categories": {
                    "crypto": {"action": "FOLLOW", "action_confidence": 0.9}
                }
            }
        }, tmp_path)
        wrong_path = _make_classifications_json({}, tmp_path / "wrong")
        monkeypatch.setattr(wfca, "_CLASSIFICATIONS_PATH", real_path)

        # Pass a non-existent path as classifications_path — it should be ignored
        result = wfca.get_category_action("WhaleN", "crypto", classifications_path="/nonexistent/path.json")

        assert result["action"] == "FOLLOW"

    def test_classifications_path_parameter_does_not_affect_db_path(self, monkeypatch, tmp_path):
        """
        BROKEN CURRENT BEHAVIOR: classifications_path is supposed to override
        the data path, but _load_classifications() ignores it (always reads
        _CLASSIFICATIONS_PATH). is_whale_active() also ignores it for the
        discovery DB (always uses _DISCOVERY_DB_PATH). The parameter is
        non-functional for both JSON and DB paths.
        """
        _clear_all_caches()
        json_path = _make_classifications_json({
            "WhaleO": {
                "global": {
                    "global_action": "FOLLOW",
                    "global_action_confidence": 0.8,
                }
            }
        }, tmp_path)
        monkeypatch.setattr(wfca, "_CLASSIFICATIONS_PATH", json_path)
        # Force whale active to isolate the classifications_path test from activity gate
        monkeypatch.setattr(wfca, "is_whale_active", lambda *a, **k: (True, "test_active"))

        # classifications_path is passed but ignored — _load_classifications()
        # still reads _CLASSIFICATIONS_PATH, so the real JSON is used.
        result = wfca.get_category_action("WhaleO", "crypto", classifications_path="/nonexistent/path.json")
        assert result["action"] == "FOLLOW"
        assert result["source"] == "global_fallback"
