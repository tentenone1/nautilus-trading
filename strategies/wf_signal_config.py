"""Whale Follower — Signal config loading with hot-reload.

Loads manipulation playbook, whale profiles, and jailbreak strategies
from JSON files with mtime-based hot-reload support.
"""

from __future__ import annotations

import json
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths to JSON configuration files (relative to repository root)
# ---------------------------------------------------------------------------
_MANIP_PLAYBOOK_PATH = Path(__file__).resolve().parents[2] / "research" / "manipulation_playbook.json"
_WHALE_PROFILES_PATH = Path(__file__).resolve().parents[2] / "research" / "whale_profiles.json"
_JAILBREAK_PATH = Path(__file__).resolve().parents[2] / "research" / "jailbreak_strategies.json"

# ---------------------------------------------------------------------------
# Hot‑reload support for static JSON configs
# ---------------------------------------------------------------------------
_MANIP_MTIME: float | None = None
_WHALE_PROFILES_MTIME: float | None = None
_JAILBREAK_MTIME: float | None = None

# Backward‑compatible module‑level data containers
_MANIPULATION_PLAYBOOK: dict = {"tactics": []}
_WHALE_PROFILES: dict = {"profiles": []}
_JAILBREAK_STRATEGIES: dict = {"strategies": []}


def _load_manip_playbook(force: bool = False) -> dict:
    """Load (and hot‑reload if file mtime changed) the manipulation playbook.

    Args:
        force: If ``True`` ignore the cached mtime and reload unconditionally.
    Returns:
        The parsed JSON dictionary representing the manipulation playbook.
    """
    global _MANIPULATION_PLAYBOOK, _MANIP_MTIME
    try:
        mtime = _MANIP_PLAYBOOK_PATH.stat().st_mtime
        if force or _MANIP_MTIME is None or mtime > _MANIP_MTIME:
            with open(_MANIP_PLAYBOOK_PATH, "r", encoding="utf-8") as fh:
                _MANIPULATION_PLAYBOOK = json.load(fh)
            _MANIP_MTIME = mtime
    except FileNotFoundError:
        _MANIPULATION_PLAYBOOK = {"tactics": []}
    except Exception:
        # Preserve previous good data on unexpected errors
        pass
    return _MANIPULATION_PLAYBOOK


def _load_whale_profiles(force: bool = False) -> dict:
    """Load (and hot‑reload if file mtime changed) the whale profiles.

    Args:
        force: If ``True`` reload even if the file timestamp has not changed.
    Returns:
        The parsed JSON dictionary of whale profiles.
    """
    global _WHALE_PROFILES, _WHALE_PROFILES_MTIME
    try:
        mtime = _WHALE_PROFILES_PATH.stat().st_mtime
        if force or _WHALE_PROFILES_MTIME is None or mtime > _WHALE_PROFILES_MTIME:
            with open(_WHALE_PROFILES_PATH, "r", encoding="utf-8") as fh:
                _WHALE_PROFILES = json.load(fh)
            _WHALE_PROFILES_MTIME = mtime
    except FileNotFoundError:
        _WHALE_PROFILES = {"profiles": []}
    except Exception:
        pass
    return _WHALE_PROFILES


def _load_jailbreak_strategies(force: bool = False) -> dict:
    """Load (and hot‑reload) the jailbreak strategies JSON.

    Args:
        force: Reload regardless of modification time.
    Returns:
        Parsed JSON dictionary of jailbreak strategies.
    """
    global _JAILBREAK_STRATEGIES, _JAILBREAK_MTIME
    try:
        mtime = _JAILBREAK_PATH.stat().st_mtime
        if force or _JAILBREAK_MTIME is None or mtime > _JAILBREAK_MTIME:
            with open(_JAILBREAK_PATH, "r", encoding="utf-8") as fh:
                _JAILBREAK_STRATEGIES = json.load(fh)
            _JAILBREAK_MTIME = mtime
    except FileNotFoundError:
        _JAILBREAK_STRATEGIES = {"strategies": []}
    except Exception:
        pass
    return _JAILBREAK_STRATEGIES


def _reload_all_configs(force: bool = True) -> None:
    """Hot‑reload all JSON configs.

    This function can be called at module import time or on SIGHUP to pick up
    configuration changes without restarting the process.
    """
    _load_manip_playbook(force=force)
    _load_whale_profiles(force=force)
    _load_jailbreak_strategies(force=force)

# Initial load when the module is imported
_load_manip_playbook()
_load_whale_profiles()
_load_jailbreak_strategies()

# Export public names for other modules
load_manip_playbook = _load_manip_playbook
load_whale_profiles = _load_whale_profiles
load_jailbreak_strategies = _load_jailbreak_strategies
reload_all_configs = _reload_all_configs
MANIPULATION_PLAYBOOK = _MANIPULATION_PLAYBOOK
WHALE_PROFILES = _WHALE_PROFILES
JAILBREAK_STRATEGIES = _JAILBREAK_STRATEGIES
