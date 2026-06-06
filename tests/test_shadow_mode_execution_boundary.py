"""
Tests for SHADOW_MODE execution boundary.

Verifies:
1. SHADOW_MODE constant is True (current baseline).
2. run_micro_live.py contains the hard SHADOW_MODE sys.exit guard.
3. PositionManager.enter_position() shadows before submit_order.
4. No submit_order call happens when SHADOW_MODE blocks.
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from strategies.wf_constants import SHADOW_MODE


class TestShadowModeConstant(unittest.TestCase):
    def test_shadow_mode_is_true(self):
        self.assertTrue(SHADOW_MODE)


class TestRunMicroLiveGuard(unittest.TestCase):
    """Source-level test: run_micro_live.py must contain the SHADOW_MODE guard."""

    @classmethod
    def setUpClass(cls):
        cls.source_path = os.path.join(
            os.path.dirname(__file__), "..", "run_micro_live.py"
        )
        with open(cls.source_path) as f:
            cls.source = f.read()

    def test_imports_shadow_mode(self):
        self.assertIn("from strategies.wf_constants import SHADOW_MODE", self.source)

    def test_checks_shadow_mode(self):
        self.assertIn("SHADOW_MODE", self.source)

    def test_exits_on_shadow_mode(self):
        self.assertIn("sys.exit(1)", self.source)

    def test_guard_precedes_execution_code(self):
        """Guard block appears before the CLOB client initialization."""
        guard_idx = self.source.find("if SHADOW_MODE:")
        clob_idx = self.source.find("ClobClient")
        self.assertGreater(guard_idx, 0, "SHADOW_MODE guard not found")
        self.assertGreater(
            clob_idx, guard_idx,
            "SHADOW_MODE guard must appear before ClobClient initialization",
        )


class TestPositionManagerShadowGate(unittest.TestCase):
    """Source-level regression test for the SHADOW_MODE gate in enter_position."""

    @classmethod
    def setUpClass(cls):
        cls.source_path = os.path.join(
            os.path.dirname(__file__), "..", "strategies", "position_manager.py"
        )
        with open(cls.source_path) as f:
            cls.source = f.read()

    def test_shadow_mode_block_appears_before_submit_order(self):
        """The SHADOW_MODE return must precede submit_order in enter_position."""
        shadow_idx = self.source.find("if SHADOW_MODE:")
        submit_idx = self.source.find("s.submit_order(order)")
        self.assertGreater(shadow_idx, 0, "SHADOW_MODE gate not found in position_manager.py")
        self.assertGreater(submit_idx, 0, "submit_order call not found in position_manager.py")
        self.assertGreater(
            submit_idx, shadow_idx,
            "SHADOW_MODE gate must appear before submit_order in enter_position",
        )

    def test_shadow_block_does_not_call_submit_order(self):
        """Inside the J1 execution-boundary SHADOW_MODE block, no submit_order."""
        j1_idx = self.source.find("J1 CRITICAL: SHADOW_MODE enforcement")
        self.assertGreater(j1_idx, 0, "J1 comment not found")
        block_start = self.source.find("if SHADOW_MODE:", j1_idx)
        self.assertGreater(block_start, 0, "SHADOW_MODE gate after J1 not found")
        shadow_block = self.source[block_start:]
        # return inside the SHADOW_MODE block is at 12-space indent
        return_idx = shadow_block.find("\n            return")
        submit_in_block = shadow_block[:return_idx].find("submit_order")
        self.assertEqual(
            submit_in_block, -1,
            "submit_order must not appear inside the SHADOW_MODE block",
        )

    def test_shadow_mode_records_shadow_trade(self):
        """The SHADOW_MODE block must set final_decision to SHADOW_TRADE."""
        self.assertIn('"SHADOW_TRADE"', self.source)
        self.assertIn('"shadow_mode_block"', self.source)

    def test_enter_position_has_shadow_mode_doc_comment(self):
        """The critical J1 comment should appear near the SHADOW_MODE gate."""
        self.assertIn("J1 CRITICAL: SHADOW_MODE enforcement", self.source)


class TestPositionManagerImportsShadowMode(unittest.TestCase):
    """Position manager imports the same SHADOW_MODE constant used by the safety gate."""

    def test_position_manager_imports_shadow_mode_constant(self):
        from strategies import position_manager

        self.assertIs(position_manager.SHADOW_MODE, SHADOW_MODE)
        self.assertTrue(position_manager.SHADOW_MODE)


if __name__ == "__main__":
    unittest.main()
