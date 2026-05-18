"""Tests for db_router module."""

import os
import sys
from pathlib import Path
import tempfile

# Load db_router module directly
_db_router_path = Path(__file__).resolve().parent.parent / "components" / "validation" / "db_router.py"
_spec = __import__("importlib.util").util.spec_from_file_location("db_router", _db_router_path)
db_router_module = __import__("importlib.util").util.module_from_spec(_spec)
sys.modules["db_router"] = db_router_module
_spec.loader.exec_module(db_router_module)

DatabaseRouter = db_router_module.DatabaseRouter


def test_get_db_path_returns_correct_paths():
    """Test that router returns correct paths for each mode."""
    with tempfile.TemporaryDirectory() as tmpdir:
        router = DatabaseRouter(base_dir=Path(tmpdir))
        
        paper_path = router.get_db_path("paper")
        live_path = router.get_db_path("live")
        replay_path = router.get_db_path("replay")
        
        assert paper_path.name == "/home/elon-1/workspace/nautilus-trading/data/trades.db"
        assert live_path.name == "/home/elon-1/workspace/nautilus-trading/data/trades.db"
        assert replay_path.name == "/home/elon-1/workspace/nautilus-trading/data/trades.db"
        
        assert paper_path.parent == Path(tmpdir)
        assert live_path.parent == Path(tmpdir)
        assert replay_path.parent == Path(tmpdir)


def test_invalid_mode_raises_error():
    """Test that invalid mode raises ValueError."""
    router = DatabaseRouter()
    
    try:
        router.get_db_path("invalid")
        assert False, "Should have raised ValueError"
    except ValueError as e:
        assert "Invalid mode" in str(e)


def test_current_mode_from_env():
    """Test that mode can be set via environment."""
    router = DatabaseRouter()
    
    # Set env
    os.environ["TRADE_MODE"] = "live"
    
    assert router.get_current_mode() == "live"
    assert router.get_current_db_path().name == "/home/elon-1/workspace/nautilus-trading/data/trades.db"
    
    # Clean up
    del os.environ["TRADE_MODE"]


def test_default_mode_is_paper():
    """Test that default mode is paper when env not set."""
    # Clear env if set
    if "TRADE_MODE" in os.environ:
        del os.environ["TRADE_MODE"]
    
    router = DatabaseRouter(default_mode="paper")
    
    assert router.get_current_mode() == "paper"
    assert router.get_current_db_path().name == "/home/elon-1/workspace/nautilus-trading/data/trades.db"


def test_set_mode_changes_env():
    """Test that set_mode updates environment."""
    router = DatabaseRouter()
    
    router.set_mode("replay")
    
    assert os.environ.get("TRADE_MODE") == "replay"
    assert router.get_current_mode() == "replay"
    
    # Clean up
    del os.environ["TRADE_MODE"]


def test_get_all_db_paths():
    """Test that all paths can be retrieved."""
    with tempfile.TemporaryDirectory() as tmpdir:
        router = DatabaseRouter(base_dir=Path(tmpdir))
        
        all_paths = router.get_all_db_paths()
        
        assert "paper" in all_paths
        assert "live" in all_paths
        assert "replay" in all_paths
        
        assert all_paths["paper"].name == "/home/elon-1/workspace/nautilus-trading/data/trades.db"
        assert all_paths["live"].name == "/home/elon-1/workspace/nautilus-trading/data/trades.db"
        assert all_paths["replay"].name == "/home/elon-1/workspace/nautilus-trading/data/trades.db"


def test_base_dir_is_created():
    """Test that base directory is created if it doesn't exist."""
    with tempfile.TemporaryDirectory() as tmpdir:
        new_dir = Path(tmpdir) / "new_research"
        assert not new_dir.exists()
        
        DatabaseRouter(base_dir=new_dir)
        
        assert new_dir.exists()