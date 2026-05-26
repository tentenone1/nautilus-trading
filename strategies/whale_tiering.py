"""Dual-Axis Whale Tiering Configuration and Risk Limits.

Classifies whales on two axes:
  CAPITAL (A-E): Total position volume on Polymarket
  PRECISION (HIGH/MED/LOW): Actual resolution win rate

Replaces the old single-axis alpha_score system.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any


class WhaleTiering:
    """Dual-axis whale classification: Capital × Precision.

    Reads tier matrix from config and provides methods to classify
    whales, validate signals, and calculate position sizes.
    Falls back to single-axis (alpha_score) for backward compat.
    """

    def __init__(self, config_path: str = "config/whale_tiers.json") -> None:
        self._config_path = Path(config_path)
        if not self._config_path.is_absolute():
            self._config_path = Path(__file__).resolve().parent.parent / self._config_path
        self._config: dict[str, Any] = {}
        self._whale_cache: dict[str, dict[str, Any]] = {}
        self._tier_assignments: dict[str, dict[str, Any]] = {}
        self._load_config()
        self._load_tier_assignments()

    def _load_tier_assignments(self) -> None:
        """Load persistent wallet-to-tier assignments from config/tier_assignments.json."""
        ta_path = self._config_path.parent / "tier_assignments.json"
        if ta_path.exists():
            with open(ta_path) as f:
                self._tier_assignments = json.load(f)
            # Pre-populate cache from tier assignments
            for name, tier_data in self._tier_assignments.items():
                if name not in self._whale_cache:
                    self._whale_cache[name] = {
                        "capital_tier": tier_data.get("capital_tier", "E"),
                        "precision_tier": tier_data.get("precision_tier", "LOW"),
                        "kelly_multiplier": tier_data.get("kelly_multiplier", 0.5),
                        "classification": tier_data.get("classification", "unknown"),
                    }

    def _load_config(self) -> None:
        if self._config_path.exists():
            with open(self._config_path) as f:
                self._config = json.load(f)
        else:
            self._config = {}

    # ── Dual-Axis Classification ────────────────────────────────────────

    def classify_capital(self, total_volume_usd: float) -> str:
        """Classify by total position volume. Returns A/B/C/D/E."""
        tiers = self._config.get("capital_tiers", {})
        for label in ("A", "B", "C", "D", "E"):
            cfg = tiers.get(label, {})
            if cfg.get("min_volume", 0) <= total_volume_usd <= cfg.get("max_volume", 999999999):
                return label
        return "E"

    def classify_precision(self, win_rate: float) -> str:
        """Classify by resolution win rate. Returns HIGH/MEDIUM/LOW."""
        tiers = self._config.get("precision_tiers", {})
        for label in ("HIGH", "MEDIUM", "LOW"):
            if win_rate >= tiers.get(label, {}).get("min_wr", 0):
                return label
        return "LOW"

    def get_category_multiplier(self, market_category: str) -> dict[str, float]:
        """Return Kelly adjustment for a market category. Third axis of tiering."""
        cat_multipliers = self._config.get("category_multipliers", {})
        return cat_multipliers.get(market_category, cat_multipliers.get("unknown", {"kelly_adjustment": 0.5, "max_position_usd_multiplier": 0.5}))

    def apply_category_adjustment(self, tier_config: dict[str, Any], market_category: str) -> dict[str, Any]:
        """Apply category-based adjustments to a tier config."""
        cat_mult = self.get_category_multiplier(market_category)
        adjusted = tier_config.copy()
        adjusted["kelly_multiplier"] *= cat_mult.get("kelly_adjustment", 1.0)
        adjusted["max_position_usd"] = int(adjusted.get("max_position_usd", 100) * cat_mult.get("max_position_usd_multiplier", 1.0))
        adjusted["category"] = market_category
        return adjusted

    def get_dual_tier(self, capital_tier: str, precision_tier: str) -> str:
        """Return combined tier key like 'B+HIGH'."""
        return f"{capital_tier}+{precision_tier}"

    def get_dual_tier_config(self, capital_tier: str, precision_tier: str) -> dict[str, Any]:
        """Return config for a dual-axis tier combination."""
        matrix = self._config.get("tier_matrix", {})
        key = self.get_dual_tier(capital_tier, precision_tier)
        return matrix.get(key, self._get_fallback_config())

    def _get_fallback_config(self) -> dict[str, Any]:
        """Fallback if tier matrix key is missing."""
        return {
            "kelly_multiplier": 0.25,
            "max_position_usd": 100,
            "max_concurrent": 2,
            "min_confidence": 0.5,
        }

    def cache_whale(self, whale_name: str, total_volume_usd: float, win_rate: float) -> None:
        """Pre-classify a whale and cache the result."""
        cap = self.classify_capital(total_volume_usd)
        prec = self.classify_precision(win_rate)
        self._whale_cache[whale_name] = {
            "capital_tier": cap,
            "precision_tier": prec,
            "volume": total_volume_usd,
            "win_rate": win_rate,
        }

    def get_cached_tier(self, whale_name: str) -> dict[str, Any]:
        """Return cached tier data for a whale, or fallback defaults."""
        cached = self._whale_cache.get(whale_name)
        if cached:
            return self.get_dual_tier_config(cached["capital_tier"], cached["precision_tier"])
        return self._get_fallback_config()

    def get_raw_cache(self, whale_name: str) -> dict[str, Any] | None:
        """Return raw cached tier data (capital_tier, precision_tier, volume, win_rate)."""
        return self._whale_cache.get(whale_name)

    # ── Backward Compat: Single-Axis (alpha_score) ─────────────────────

    def get_tier(self, alpha_score: float) -> str:
        """Legacy single-axis tier. Returns tier name from backwards_compat config."""
        tiers = self._config.get("backwards_compat", {}).get("tiers", {})
        for tier_name, cfg in sorted(tiers.items(), key=lambda x: -x[1].get("alpha_min", 0)):
            if cfg["alpha_min"] <= alpha_score <= cfg["alpha_max"]:
                return tier_name
        return "speculative"

    def get_tier_config(self, alpha_score: float) -> dict[str, Any]:
        """Legacy single-axis config lookup."""
        tier_name = self.get_tier(alpha_score)
        tiers = self._config.get("backwards_compat", {}).get("tiers", {})
        return tiers.get(tier_name, self._config.get("backwards_compat", {}).get("defaults", {}))

    def apply_overrides(self, tier_config: dict[str, Any], tags: list[str] | None = None) -> dict[str, Any]:
        """Legacy tag-based overrides."""
        result = dict(tier_config)
        if not tags:
            return result
        overrides = self._config.get("backwards_compat", {}).get("overrides", {}).get("tag_based", {})
        for tag in tags:
            if tag in overrides:
                override = overrides[tag]
                if "min_confidence_reduction" in override:
                    current = result.get("min_confidence", 0.3)
                    result["min_confidence"] = round(max(0.1, current - override["min_confidence_reduction"]), 2)
        return result

    def kelly_sized_position(self, base_kelly: float, alpha_score: float, tags: list[str] | None = None) -> float:
        """Legacy: size by alpha_score tier."""
        tier_config = self.get_tier_config(alpha_score)
        tier_config = self.apply_overrides(tier_config, tags)
        kelly_mult = tier_config.get("kelly_multiplier", 0.5)
        return base_kelly * kelly_mult

    def validate_confidence(self, confidence: float, alpha_score: float, tags: list[str] | None = None) -> bool:
        """Legacy: validate by alpha_score tier."""
        tier_config = self.get_tier_config(alpha_score)
        tier_config = self.apply_overrides(tier_config, tags)
        min_conf = tier_config.get("min_confidence", 0.3)
        return confidence >= min_conf

    def validate_edge_score(self, edge_score: float, alpha_score: float) -> bool:
        """Legacy: validate edge by alpha_score tier."""
        tier_config = self.get_tier_config(alpha_score)
        min_edge = tier_config.get("min_edge_score", 0.15)
        return edge_score >= min_edge

    def get_edge_kelly(self, edge_score: float) -> float:
        """Legacy: map edge_score to Kelly fraction."""
        self._load_config()
        mapping = self._config.get("backwards_compat", {}).get("edge_kelly_mapping", {})
        ranges = mapping.get("ranges", [])
        default_kelly = mapping.get("default_kelly_fraction", 0.05)
        for rng in ranges:
            if rng["min"] <= edge_score < rng["max"]:
                return rng["kelly_fraction"]
        return default_kelly

    def get_sanity_checks(self) -> dict[str, Any]:
        """Get position sizing sanity check bounds."""
        self._load_config()
        sanity = self._config.get("backwards_compat", {}).get("kelly_sanity_checks", {})
        return {
            "max_position_pct": sanity.get("max_position_pct", 0.125),
            "min_position_pct": sanity.get("min_position_pct", 0.005),
            "enabled": sanity.get("enabled", True),
        }

    # ── New Dual-Axis: Kelly Sizing ─────────────────────────────────────

    def dual_kelly_sized_position(self, base_kelly: float, whale_name: str) -> float:
        """Size position using dual-axis cached tier data."""
        config = self.get_cached_tier(whale_name)
        kelly_mult = config.get("kelly_multiplier", 0.25)
        return base_kelly * kelly_mult

    def get_dual_sizing_limits(self, whale_name: str) -> dict[str, Any]:
        """Return sizing limits from dual-axis tier."""
        config = self.get_cached_tier(whale_name)
        return {
            "max_position_usd": config.get("max_position_usd", 100),
            "max_concurrent": config.get("max_concurrent", 2),
            "min_confidence": config.get("min_confidence", 0.5),
        }

    def get_all_tier_summary(self) -> dict[str, Any]:
        """Return full tier matrix for display/reporting."""
        return {
            "capital_tiers": self._config.get("capital_tiers", {}),
            "precision_tiers": self._config.get("precision_tiers", {}),
            "tier_matrix": self._config.get("tier_matrix", {}),
        }


class WhaleIntelligence:
    """Load and query whale intelligence data for signal filtering and Kelly adjustments."""

    KELLY_MAP: dict[str, float] = {
        "skilled_human": 1.25,
        "trading_bot": 1.0,
        "market_maker": 0.75,
        "mixed_entity": 0.9,
        "degenerate_human": 0.5,
        "sacrificial_account": 0.0,
    }

    def __init__(self, db_path: str | None = None) -> None:
        """Load whale intelligence from DB into memory cache."""
        self._intel: dict[str, dict] = {}
        if db_path is None:
            base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            db_path = os.path.join(base, "pipeline", "data", "whale_discovery.db")
        if not os.path.exists(db_path):
            return
        try:
            import sqlite3
            conn = sqlite3.connect(db_path)
            rows = conn.execute(
                "SELECT name, classification, trust_score, should_copy, should_fade, volume, win_rate FROM whale_intelligence"
            ).fetchall()
            conn.close()
            for r in rows:
                self._intel[r[0]] = {
                    "classification": r[1],
                    "trust_score": r[2],
                    "should_copy": bool(r[3]),
                    "should_fade": bool(r[4]),
                    "volume": r[5],
                    "win_rate": r[6],
                }
        except Exception:
            pass

    def get(self, whale_name: str) -> dict | None:
        """Return intelligence dict for a whale, or None if unknown."""
        return self._intel.get(whale_name)

    def is_known(self, whale_name: str) -> bool:
        """Check if whale exists in intelligence database."""
        return whale_name in self._intel

    def should_hard_reject(self, whale_name: str) -> bool:
        """Return True if this whale should be rejected as a sacrificial account."""
        intel = self._intel.get(whale_name)
        if not intel:
            return False
        return (
            intel["classification"] == "sacrificial_account"
            and intel["trust_score"] <= 3
            and intel["should_fade"]
        )

    def should_fade(self, whale_name: str) -> dict | None:
        """Return intel dict if whale should be faded, None otherwise.
        
        Fade candidates: should_fade=1 AND classification != "sacrificial_account".
        Trust score is informational only — losing whales have low trust by definition.
        """
        intel = self._intel.get(whale_name)
        if not intel:
            return None
        if intel["classification"] == "sacrificial_account":
            return None  # Hard-reject these, don't fade
        if intel.get("should_fade"):
            return intel
        return None


    def apply_size_modifier(self, whale_name: str, base_size: float, category: str = "") -> tuple[float, str]:
        """Adjust position size based on whale trust score."""
        intel = self._intel.get(whale_name)
        if not intel:
            return base_size, "no_intel_data"
        trust = intel.get("trust_score", 5.0)
        trust_mult = max(0.25, min(2.0, trust / 5.0))
        new_size = round(base_size * trust_mult, 2)
        note = f"trust={trust:.1f} mult={trust_mult:.2f}" if abs(trust_mult - 1.0) > 0.05 else ""
        return new_size, note

    def get_follow_list(self, min_trust: float = 5.0, min_volume: float = 1000.0) -> list[dict]:
        """Get dynamic whale following list based on intelligence data.

        Replaces hardcoded follow list with selection from 420 classified profiles.

        Args:
            min_trust: Minimum trust score to include (default 5.0).
            min_volume: Minimum volume to include (default 1000.0).

        Returns:
            List of dicts with name, classification, trust_score, volume, win_rate.
            Excludes sacrificial_account and should_fade whales.
        """
        follow = []
        for name, intel in self._intel.items():
            if intel.get("classification") == "sacrificial_account":
                continue
            if intel.get("should_fade"):
                continue
            trust = intel.get("trust_score", 0) or 0
            vol = intel.get("volume", 0) or 0
            if trust >= min_trust and vol >= min_volume:
                follow.append({
                    "name": name,
                    "classification": intel.get("classification", "unknown"),
                    "trust_score": trust,
                    "volume": vol,
                    "win_rate": intel.get("win_rate", 0) or 0,
                })
        follow.sort(key=lambda x: x["trust_score"], reverse=True)
        return follow

    def kelly_multiplier(self, classification: str) -> float:
        """Return Kelly multiplier for a given classification."""
        return self.KELLY_MAP.get(classification, 1.0)

    def bulk_cache_tiers(self, tiering: "WhaleTiering") -> int:
        """Classify all loaded whales into dual-axis tiers and cache them in WhaleTiering.
        Returns count of successfully cached whales."""
        cached = 0
        for name, intel in self._intel.items():
            volume = intel.get("volume", 0) or 0
            win_rate = intel.get("win_rate", 0) or 0
            if volume > 0 and win_rate > 0:
                tiering.cache_whale(name, volume, win_rate)
                cached += 1
        print(f"[WhaleIntelligence] Cached {cached}/{len(self._intel)} whales into dual-axis tiers")
        return cached

    def adjust_size(self, whale_name: str, size_usd: float) -> tuple[float, dict | None]:
        """Apply classification-based Kelly multiplier to position size.
        
        Returns:
            Tuple of (adjusted_size, intel_dict_or_None).
        """
        intel = self._intel.get(whale_name)
        if not intel:
            return size_usd, None
        mult = self.kelly_multiplier(intel["classification"])
        return round(size_usd * mult, 2), intel
