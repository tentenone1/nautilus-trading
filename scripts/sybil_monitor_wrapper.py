"""Sybil‑intelligence orchestration wrapper.

Provides run_sybil_monitoring() that orchestrates the three existing
sybil utilities and writes a unified report to research/sybil_intelligence.json.
"""

import json
import logging
import os
import pathlib
from datetime import datetime, timezone
from typing import Any, Dict

from scripts.sybil_config import get_config

LOGGER = logging.getLogger(__name__)
config = get_config()


def _safe_call(func: callable, name: str) -> Dict[str, Any]:
    """Execute function with error handling."""
    try:
        result = func()
        LOGGER.debug("%s returned %s", name, result)
        return result or {}
    except Exception as exc:
        LOGGER.error("%s failed: %s", name, exc)
        return {}


def run_sybil_monitoring() -> Dict[str, Any]:
    """Run the three sybil components and write a unified JSON report.

    Returns:
        dict: Combined report structure with signals, positions, and meta_whale_groups.
    """
    # Import existing sybil scripts
    try:
        from scripts.sybil_signal_generator import main as gen_signal
        from scripts.sybil_position_aggregator import main as agg_position
        from scripts.sybil_intelligence import main as compute_meta
    except ImportError as e:
        LOGGER.warning("Sybil imports failed: %s", e)
        return {"error": str(e), "timestamp": datetime.now(timezone.utc).isoformat()}

    # Step 1 – generate raw signals
    signals = _safe_call(gen_signal, "sybil_signal_generator")

    # Step 2 – aggregate positions
    positions = _safe_call(agg_position, "sybil_position_aggregator")

    # Step 3 – compute meta‑whale groups
    # Pass default args namespace to avoid sys.argv parsing conflict
    import argparse as _argparse
    _sybil_args = _argparse.Namespace(full_history=False, output=str(pathlib.Path(__file__).parent.parent / "research" / "sybil_intelligence.json"), skip_llm=False)
    meta = _safe_call(lambda: compute_meta(args=_sybil_args), "sybil_intelligence")

    report = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "signals": signals,
        "positions": positions,
        "meta_whale_groups": meta,
    }

    # Write report
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    out_path = pathlib.Path(base_dir) / config.paths.research_dir / config.paths.intelligence_file
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2))
    LOGGER.info("Sybil intelligence report written to %s", out_path)

    return report


if __name__ == "__main__":
    print(json.dumps(run_sybil_monitoring(), indent=2))