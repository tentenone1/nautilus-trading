"""
Smoke tests for active v6.6 reporting scripts.

Verifies each script:
1. Exists on disk.
2. Can be imported or py_compile'd without executing network calls.
3. Exposes expected functions (main, generate_report, render_markdown).
"""

import os
import py_compile
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

SCRIPTS_DIR = os.path.join(os.path.dirname(__file__), "..", "scripts")


def _script_path(name: str) -> str:
    return os.path.join(SCRIPTS_DIR, name)


class TestReportV66PaperPortfolio(unittest.TestCase):
    """Smoke tests for scripts/report_v66_paper_portfolio.py."""

    @classmethod
    def setUpClass(cls):
        cls.path = _script_path("report_v66_paper_portfolio.py")
        cls.exists = os.path.exists(cls.path)

    def test_file_exists(self):
        self.assertTrue(self.exists, "report_v66_paper_portfolio.py not found")

    def test_py_compile(self):
        if not self.exists:
            self.skipTest("file not found")
        try:
            py_compile.compile(self.path, doraise=True)
        except py_compile.PyCompileError as e:
            self.fail(f"Compile failed: {e}")

    def test_importable(self):
        if not self.exists:
            self.skipTest("file not found")
        try:
            from scripts import report_v66_paper_portfolio as mod
            self.assertTrue(hasattr(mod, "generate_report"))
            self.assertTrue(hasattr(mod, "render_markdown"))
        except Exception as e:
            self.fail(f"Import failed: {e}")


class TestReportV66MtmCoverage(unittest.TestCase):
    """Smoke tests for scripts/report_v66_mtm_coverage.py."""

    @classmethod
    def setUpClass(cls):
        cls.path = _script_path("report_v66_mtm_coverage.py")
        cls.exists = os.path.exists(cls.path)

    def test_file_exists(self):
        self.assertTrue(self.exists, "report_v66_mtm_coverage.py not found")

    def test_py_compile(self):
        if not self.exists:
            self.skipTest("file not found")
        try:
            py_compile.compile(self.path, doraise=True)
        except py_compile.PyCompileError as e:
            self.fail(f"Compile failed: {e}")

    def test_importable(self):
        if not self.exists:
            self.skipTest("file not found")
        try:
            from scripts import report_v66_mtm_coverage as mod
            self.assertTrue(hasattr(mod, "generate_report") or hasattr(mod, "main"))
        except Exception as e:
            self.fail(f"Import failed: {e}")


class TestUpdateV66PaperPortfolio(unittest.TestCase):
    """Smoke tests for scripts/update_v66_paper_portfolio.py."""

    @classmethod
    def setUpClass(cls):
        cls.path = _script_path("update_v66_paper_portfolio.py")
        cls.exists = os.path.exists(cls.path)

    def test_file_exists(self):
        self.assertTrue(self.exists, "update_v66_paper_portfolio.py not found")

    def test_py_compile(self):
        if not self.exists:
            self.skipTest("file not found")
        try:
            py_compile.compile(self.path, doraise=True)
        except py_compile.PyCompileError as e:
            self.fail(f"Compile failed: {e}")

    def test_importable(self):
        if not self.exists:
            self.skipTest("file not found")
        try:
            from scripts import update_v66_paper_portfolio as mod
            self.assertTrue(hasattr(mod, "main") or hasattr(mod, "run_update"))
        except Exception as e:
            self.fail(f"Import failed: {e}")

    def test_contains_dry_run_flag(self):
        if not self.exists:
            self.skipTest("file not found")
        with open(self.path) as f:
            content = f.read()
        self.assertIn("--dry-run", content)
        self.assertIn("dry_run", content)


class TestPollShadowTrades(unittest.TestCase):
    """Smoke tests for scripts/poll_shadow_trades.py."""

    @classmethod
    def setUpClass(cls):
        cls.path = _script_path("poll_shadow_trades.py")
        cls.exists = os.path.exists(cls.path)

    def test_file_exists(self):
        self.assertTrue(self.exists, "poll_shadow_trades.py not found")

    def test_py_compile(self):
        if not self.exists:
            self.skipTest("file not found")
        try:
            py_compile.compile(self.path, doraise=True)
        except py_compile.PyCompileError as e:
            self.fail(f"Compile failed: {e}")

    def test_importable(self):
        if not self.exists:
            self.skipTest("file not found")
        try:
            from scripts import poll_shadow_trades as mod
            self.assertTrue(hasattr(mod, "main"))
        except Exception as e:
            self.fail(f"Import failed: {e}")

    def test_contains_dry_run_flag(self):
        if not self.exists:
            self.skipTest("file not found")
        with open(self.path) as f:
            content = f.read()
        self.assertIn("--dry-run", content)
        self.assertIn("dry_run", content)


if __name__ == "__main__":
    unittest.main()
