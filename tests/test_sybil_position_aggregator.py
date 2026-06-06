"""
Unit tests for sybil_position_aggregator safety patch.

Tests:
1. TRADES_DB resolves to the repo-local data/trades.db path.
2. Compound wallet suffixes are normalized to clean addresses.
3. HTTP 400 errors are handled and tracked.
4. HTTP/SSL/network errors do not crash the aggregator.
"""

import json
import os
import sys
import unittest
from unittest.mock import patch, MagicMock
from urllib.error import HTTPError, URLError
import ssl

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from scripts.sybil_position_aggregator import (
    TRADES_DB,
    BASE_DIR,
    fetch_positions,
    _KNOWN_BAD_ADDRESSES,
)


class TestTradesDbPath(unittest.TestCase):
    def test_trades_db_resolves_to_repo_data(self):
        expected = os.path.join(BASE_DIR, "data", "trades.db")
        self.assertEqual(TRADES_DB, expected)

    def test_trades_db_is_within_project_root(self):
        self.assertTrue(TRADES_DB.startswith(BASE_DIR))

    def test_trades_db_ends_with_correct_filename(self):
        self.assertTrue(TRADES_DB.endswith("data/trades.db"))


class TestCompoundSuffixStripping(unittest.TestCase):
    def setUp(self):
        _KNOWN_BAD_ADDRESSES.clear()

    @patch("scripts.sybil_position_aggregator.urlopen")
    def test_suffixed_address_normalizes_to_clean(self, mock_urlopen):
        mock_urlopen.return_value.__enter__.return_value.read.return_value = json.dumps([])
        result = fetch_positions("0x492442EaB586F242B53bDa933fD5dE859c8A3782-1766317541188")
        self.assertEqual(result, [])
        called_url = mock_urlopen.call_args[0][0].full_url
        self.assertIn("0x492442EaB586F242B53bDa933fD5dE859c8A3782", called_url)
        self.assertNotIn("-1766317541188", called_url)

    @patch("scripts.sybil_position_aggregator.urlopen")
    def test_clean_address_passes_through(self, mock_urlopen):
        mock_urlopen.return_value.__enter__.return_value.read.return_value = json.dumps([])
        result = fetch_positions("0x492442EaB586F242B53bDa933fD5dE859c8A3782")
        self.assertEqual(result, [])
        called_url = mock_urlopen.call_args[0][0].full_url
        self.assertIn("0x492442EaB586F242B53bDa933fD5dE859c8A3782", called_url)

    @patch("scripts.sybil_position_aggregator.urlopen")
    def test_invalid_address_returns_empty(self, mock_urlopen):
        result = fetch_positions("not-an-address")
        self.assertEqual(result, [])
        mock_urlopen.assert_not_called()

    @patch("scripts.sybil_position_aggregator.urlopen")
    def test_short_address_returns_empty(self, mock_urlopen):
        result = fetch_positions("0x123")
        self.assertEqual(result, [])
        mock_urlopen.assert_not_called()


class TestKnownBadAddresses(unittest.TestCase):
    def setUp(self):
        _KNOWN_BAD_ADDRESSES.clear()

    def tearDown(self):
        _KNOWN_BAD_ADDRESSES.clear()

    @patch("scripts.sybil_position_aggregator.urlopen")
    def test_http_400_adds_to_known_bad(self, mock_urlopen):
        mock_urlopen.side_effect = HTTPError(
            "http://example.com", 400, "Bad Request", {}, None
        )
        result = fetch_positions("0x492442EaB586F242B53bDa933fD5dE859c8A3782")
        self.assertEqual(result, [])
        self.assertIn("0x492442EaB586F242B53bDa933fD5dE859c8A3782", _KNOWN_BAD_ADDRESSES)

    @patch("scripts.sybil_position_aggregator.urlopen")
    def test_http_400_deduplicates(self, mock_urlopen):
        mock_urlopen.side_effect = HTTPError(
            "http://example.com", 400, "Bad Request", {}, None
        )
        fetch_positions("0x492442EaB586F242B53bDa933fD5dE859c8A3782")
        fetch_positions("0x492442EaB586F242B53bDa933fD5dE859c8A3782")
        self.assertEqual(len(_KNOWN_BAD_ADDRESSES), 1)
        self.assertEqual(mock_urlopen.call_count, 1)

    @patch("scripts.sybil_position_aggregator.urlopen")
    def test_http_400_with_suffix_normalizes(self, mock_urlopen):
        mock_urlopen.side_effect = HTTPError(
            "http://example.com", 400, "Bad Request", {}, None
        )
        result = fetch_positions("0x492442EaB586F242B53bDa933fD5dE859c8A3782-1766317541188")
        self.assertEqual(result, [])
        self.assertIn("0x492442EaB586F242B53bDa933fD5dE859c8A3782", _KNOWN_BAD_ADDRESSES)

    @patch("scripts.sybil_position_aggregator.urlopen")
    def test_known_bad_skips_fetch(self, mock_urlopen):
        _KNOWN_BAD_ADDRESSES.add("0x492442EaB586F242B53bDa933fD5dE859c8A3782")
        result = fetch_positions("0x492442EaB586F242B53bDa933fD5dE859c8A3782")
        self.assertEqual(result, [])
        mock_urlopen.assert_not_called()

    @patch("scripts.sybil_position_aggregator.urlopen")
    def test_known_bad_with_suffix_skips_fetch(self, mock_urlopen):
        _KNOWN_BAD_ADDRESSES.add("0x492442EaB586F242B53bDa933fD5dE859c8A3782")
        result = fetch_positions("0x492442EaB586F242B53bDa933fD5dE859c8A3782-1766317541188")
        self.assertEqual(result, [])
        mock_urlopen.assert_not_called()

    @patch("scripts.sybil_position_aggregator.urlopen")
    def test_normal_address_still_fetches_when_known_bad_exists(self, mock_urlopen):
        _KNOWN_BAD_ADDRESSES.add("0xBADBADBADBADBADBADBADBADBADBADBADBAD")
        mock_urlopen.return_value.__enter__.return_value.read.return_value = json.dumps([])
        result = fetch_positions("0x492442EaB586F242B53bDa933fD5dE859c8A3782")
        self.assertEqual(result, [])
        mock_urlopen.assert_called_once()


class TestErrorHandling(unittest.TestCase):
    def setUp(self):
        _KNOWN_BAD_ADDRESSES.clear()

    def tearDown(self):
        _KNOWN_BAD_ADDRESSES.clear()

    @patch("scripts.sybil_position_aggregator.urlopen")
    def test_http_429_does_not_crash(self, mock_urlopen):
        mock_urlopen.side_effect = HTTPError(
            "http://example.com", 429, "Too Many Requests", {}, None
        )
        result = fetch_positions("0x492442EaB586F242B53bDa933fD5dE859c8A3782")
        self.assertEqual(result, [])

    @patch("scripts.sybil_position_aggregator.urlopen")
    def test_http_404_does_not_crash(self, mock_urlopen):
        mock_urlopen.side_effect = HTTPError(
            "http://example.com", 404, "Not Found", {}, None
        )
        result = fetch_positions("0x492442EaB586F242B53bDa933fD5dE859c8A3782")
        self.assertEqual(result, [])

    @patch("scripts.sybil_position_aggregator.urlopen")
    def test_urlerror_does_not_crash(self, mock_urlopen):
        mock_urlopen.side_effect = URLError("Connection refused")
        result = fetch_positions("0x492442EaB586F242B53bDa933fD5dE859c8A3782")
        self.assertEqual(result, [])

    @patch("scripts.sybil_position_aggregator.urlopen")
    def test_oserror_does_not_crash(self, mock_urlopen):
        mock_urlopen.side_effect = OSError("Socket closed")
        result = fetch_positions("0x492442EaB586F242B53bDa933fD5dE859c8A3782")
        self.assertEqual(result, [])

    @patch("scripts.sybil_position_aggregator.urlopen")
    def test_ssl_error_does_not_crash(self, mock_urlopen):
        mock_urlopen.side_effect = ssl.SSLError("Certificate verify failed")
        result = fetch_positions("0x492442EaB586F242B53bDa933fD5dE859c8A3782")
        self.assertEqual(result, [])

    @patch("scripts.sybil_position_aggregator.urlopen")
    def test_generic_exception_does_not_crash(self, mock_urlopen):
        mock_urlopen.side_effect = RuntimeError("Unexpected error")
        result = fetch_positions("0x492442EaB586F242B53bDa933fD5dE859c8A3782")
        self.assertEqual(result, [])


if __name__ == "__main__":
    unittest.main()
