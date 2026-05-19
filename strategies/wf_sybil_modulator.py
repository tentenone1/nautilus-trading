"""Sybil Conviction Modulator for Whale Signals.

Modulates incoming whale signals using sybil group consensus data from
research/sybil_positions.json. Sybil data is advisory — it adjusts
confidence and size of real whale signals, never triggers trades on its own.

Decision matrix:
    whale YES + sybil YES ≥60%  → confidence +10%, size ×1.25  (agree)
    whale YES + sybil NO ≥70%   → SKIP  (contradicted)
    whale YES + sybil weak      → confidence −5%, size ×0.85  (uncertain)
    whale NO  + sybil NO  ≥60%  → confidence +10%, size ×1.25  (agree)
    whale NO  + sybil YES ≥70%   → SKIP  (contradicted)
    whale NO  + sybil weak       → confidence −5%, size ×0.85  (uncertain)
    no sybil data                → pass through unchanged
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

# ── Constants ────────────────────────────────────────────────────────────────

SYBIL_POSITIONS_PATH = Path(__file__).parent.parent / "research" / "sybil_positions.json"

# Conviction thresholds
SYBIL_STRONG_CONVICTION = 0.70   # ≥70% same-side wallets = strong
SYBIL_MODERATE_CONVICTION = 0.60 # ≥60% same-side wallets = moderate
SYBIL_MIN_WALLETS = 2            # Need ≥2 wallets for valid consensus
SYBIL_MIN_EXPOSURE = 1000.0      # Need ≥$1000 total exposure

# Modulation factors
SYBIL_BOOST_MULTIPLIER = 1.25    # Agree: size × 1.25
SYBIL_BOOST_ADD = 0.10           # Agree: confidence +10 pp
SYBIL_PENALTY_MULTIPLIER = 0.85  # Weak: size × 0.85
SYBIL_PENALTY_ADD = -0.05        # Weak: confidence −5 pp

# Cache freshness
_CACHE_MAX_AGE_SECS = 600.0  # Re-read file every 10 min


@dataclass(frozen=True)
class SybilModulation:
    """Result of sybil modulation check for a signal."""
    has_sybil: bool
    should_skip: bool
    confidence_delta: float     # e.g. +0.10 or -0.05
    size_multiplier: float      # e.g. 1.25 or 0.85
    sybil_ratio: float          # yes_ratio 0.0–1.0
    sybil_wallets: int
    sybil_exposure: float
    decision: str               # Human-readable decision reason


def _load_positions() -> dict:
    """Load sybil positions from disk with basic freshness guard."""
    if not SYBIL_POSITIONS_PATH.exists():
        return {}
    try:
        with open(SYBIL_POSITIONS_PATH) as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return {}


def get_sybil_consensus(condition_id: str) -> Optional[dict]:
    """Get aggregated sybil consensus across all groups for one condition_id.

    Returns dict with keys:
        sybil_side: "yes" | "no" | "mixed" | "unknown"
        sybil_ratio: float  (0.0–1.0, fraction betting YES)
        wallets: int        (total wallets across all groups)
        exposure: float      (total USD exposure)
        condition_ids: list of matched condition_ids (some markets appear in multiple groups)
    """
    data = _load_positions()
    groups: dict = data.get("groups", {})
    if not groups:
        return None

    yes_size = 0.0
    no_size = 0.0
    total_wallets = 0
    matched = False

    for group_key, group_data in groups.items():
        if not isinstance(group_data, dict):
            continue
        markets: list = group_data.get("markets", [])
        for mkt in markets:
            if not isinstance(mkt, dict):
                continue
            if mkt.get("condition_id", "").lower() != condition_id.lower():
                continue
            matched = True
            yes_size += mkt.get("yes_size_usd", 0.0)
            no_size += mkt.get("no_size_usd", 0.0)
            wallets: list = mkt.get("wallets", [])
            total_wallets += len(wallets) if isinstance(wallets, list) else 0

    if not matched:
        return None

    total = yes_size + no_size
    ratio = (yes_size / total) if total > 0 else 0.5

    if yes_size > 0 and no_size == 0:
        side = "yes"
    elif no_size > 0 and yes_size == 0:
        side = "no"
    elif yes_size > no_size:
        side = "yes"
    elif no_size > yes_size:
        side = "no"
    else:
        side = "mixed"

    return {
        "sybil_side": side,
        "sybil_ratio": ratio,
        "wallets": total_wallets,
        "exposure": total,
    }


def modulate(signal) -> SybilModulation:
    """Apply sybil conviction modulation to a whale signal.

    Returns SybilModulation with should_skip=True when the signal is
    contradicted by strong sybil consensus — the signal should be dropped.

    Does NOT modify the signal object in place — caller applies deltas.
    """
    consensus = get_sybil_consensus(getattr(signal, "condition_id", "") or "")

    if consensus is None:
        return SybilModulation(
            has_sybil=False,
            should_skip=False,
            confidence_delta=0.0,
            size_multiplier=1.0,
            sybil_ratio=0.0,
            sybil_wallets=0,
            sybil_exposure=0.0,
            decision="no_sybil_data",
        )

    ratio = consensus["sybil_ratio"]
    wallets = consensus["wallets"]
    exposure = consensus["exposure"]
    sybil_side = consensus["sybil_side"]

    # Require minimum wallets and exposure for valid consensus
    if wallets < SYBIL_MIN_WALLETS or exposure < SYBIL_MIN_EXPOSURE:
        return SybilModulation(
            has_sybil=True,
            should_skip=False,
            confidence_delta=0.0,
            size_multiplier=1.0,
            sybil_ratio=ratio,
            sybil_wallets=wallets,
            sybil_exposure=exposure,
            decision=f"sybil_weak_wallets={wallets}_exposure=${exposure:,.0f}",
        )

    whale_side = getattr(signal, "outcome", "Yes") or "Yes"
    whale_is_yes = whale_side.lower().startswith("y")
    sybil_is_yes = sybil_side == "yes"

    # ── Agrees with sybil conviction (≥60% = boost) ──────────────────────
    is_agree = whale_is_yes == sybil_is_yes
    if is_agree and ratio >= SYBIL_MODERATE_CONVICTION:
        return SybilModulation(
            has_sybil=True,
            should_skip=False,
            confidence_delta=SYBIL_BOOST_ADD,
            size_multiplier=SYBIL_BOOST_MULTIPLIER,
            sybil_ratio=ratio,
            sybil_wallets=wallets,
            sybil_exposure=exposure,
            decision=f"agree_ratio={ratio:.0%}_wallets={wallets}",
        )

    # ── Contradicted by strong sybil conviction (≥70% = skip) ───────────
    is_contradict = whale_is_yes != sybil_is_yes
    if is_contradict and ratio >= SYBIL_STRONG_CONVICTION:
        return SybilModulation(
            has_sybil=True,
            should_skip=True,
            confidence_delta=0.0,
            size_multiplier=0.0,
            sybil_ratio=ratio,
            sybil_wallets=wallets,
            sybil_exposure=exposure,
            decision=f"contradicted_ratio={ratio:.0%}_wallets={wallets}",
        )

    # ── Weak sybil consensus — penalize but don't skip ──────────────────
    return SybilModulation(
        has_sybil=True,
        should_skip=False,
        confidence_delta=SYBIL_PENALTY_ADD,
        size_multiplier=SYBIL_PENALTY_MULTIPLIER,
        sybil_ratio=ratio,
        sybil_wallets=wallets,
        sybil_exposure=exposure,
        decision=f"weak_yes={sybil_ratio:.0%}_wallets={wallets}",
    )
