#!/usr/bin/env python3
"""
Feishu Bitable Writer Module

Provides functions to write records to Feishu Bitables via lark-cli.
Uses subprocess to call the lark-cli binary for base record operations.

Bitable References:
- Research Log Bitable: KJqZbhUi2aNrebs3JzPcjMuinjg, table: 数据表
- Trading Hub Bitable: Ae2tbwT4zaaQqCsYfkGcUMdDnbb, tables: Signals, Performance
"""

import json
import logging
import subprocess
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

# Path to lark-cli binary
LARK_CLI_PATH: str = "/home/elon-1/.npm-global/bin/lark-cli"

# Bitable tokens
RESEARCH_LOG_TOKEN: str = "KJqZbhUi2aNrebs3JzPcjMuinjg"
TRADING_HUB_TOKEN: str = "Ae2tbwT4zaaQqCsYfkGcUMdDnbb"

# Table IDs
RESEARCH_LOG_TABLE: str = "数据表"
SIGNALS_TABLE: str = "Signals"
PERFORMANCE_TABLE: str = "Performance"


class BitableWriteError(Exception):
    """Exception raised when writing to Bitable fails."""
    pass


def _run_lark_cli(args: list[str], json_data: str) -> dict[str, Any]:
    """
    Execute lark-cli command with given arguments.
    
    Args:
        args: List of command-line arguments for lark-cli
        json_data: JSON string to pass as --json parameter
        
    Returns:
        Parsed JSON response from lark-cli or dict with raw output
        
    Raises:
        BitableWriteError: If command fails or returns error
    """
    cmd = [LARK_CLI_PATH] + args + ["--json", json_data]
    
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=30,
        )
        
        if result.returncode != 0:
            error_msg = result.stderr.strip() or result.stdout.strip()
            logger.error(
                "lark-cli failed with code %d: %s",
                result.returncode,
                error_msg,
                extra={"returncode": result.returncode, "error": error_msg[:200]}
            )
            raise BitableWriteError(f"lark-cli error: {error_msg[:200]}")
        
        return {"raw_output": result.stdout.strip(), "success": True}
        
    except subprocess.TimeoutExpired:
        logger.error("lark-cli command timed out", extra={"timeout": 30})
        raise BitableWriteError("lark-cli command timed out after 30 seconds")
    except FileNotFoundError:
        logger.error("lark-cli not found at %s", LARK_CLI_PATH, extra={"path": LARK_CLI_PATH})
        raise BitableWriteError(f"lark-cli binary not found at {LARK_CLI_PATH}")


def write_research_log(entry: dict[str, Any]) -> dict[str, Any]:
    """
    Write a research log entry to the Research Log Bitable.
    
    Args:
        entry: Research log entry dict with fields:
            - 文本: Text content (market + decision summary)
            - 日期: Timestamp (epoch seconds, auto-filled if missing)
            - 单选: Decision category (BUY/WAIT/SKIP)
    
    Returns:
        Response from lark-cli with success status
        
    Raises:
        BitableWriteError: If write operation fails
    """
    # Ensure timestamp
    if "日期" not in entry:
        entry["日期"] = int(datetime.now(timezone.utc).timestamp())
    
    # Validate required fields
    if "文本" not in entry:
        raise BitableWriteError("Research log entry missing '文本' field")
    
    logger.info(
        "Writing research log: %s",
        entry.get("文本", "")[:50],
        extra={"entry": entry}
    )
    
    args = [
        "base", "+record-upsert",
        "--as", "bot",
        "--base-token", RESEARCH_LOG_TOKEN,
        "--table-id", RESEARCH_LOG_TABLE,
    ]
    
    try:
        result = _run_lark_cli(args, json.dumps(entry))
        logger.info("Research log written successfully", extra={"result": result})
        return result
    except BitableWriteError:
        raise
    except Exception as e:
        logger.exception("Unexpected error writing research log")
        raise BitableWriteError(f"Failed to write research log: {e}") from e


def write_trading_signal(signal: dict[str, Any]) -> dict[str, Any]:
    """
    Write a trading signal to the Trading Hub Bitable (Signals table).
    
    Args:
        signal: Trading signal dict with fields:
            - 多行文本: Signal description
            - Confidence: 0-100 integer
            - Signal Type: Bullish/Bearish
            - Created At: Timestamp (auto-filled if missing)
            - Source: Pipeline name
            - Resolved: false
    
    Returns:
        Response from lark-cli with success status
        
    Raises:
        BitableWriteError: If write operation fails
    """
    # Ensure timestamp
    if "Created At" not in signal:
        signal["Created At"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    
    # Ensure defaults
    if "Resolved" not in signal:
        signal["Resolved"] = False
    if "Source" not in signal:
        signal["Source"] = "Autoresearch Pipeline"
    
    # Validate required fields
    if "多行文本" not in signal:
        raise BitableWriteError("Trading signal missing '多行文本' field")
    
    # Validate confidence range
    confidence = signal.get("Confidence")
    if confidence is not None:
        if not (0 <= confidence <= 100):
            raise BitableWriteError(f"Confidence must be 0-100, got {confidence}")
    
    logger.info(
        "Writing trading signal: %s",
        signal.get("多行文本", "")[:50],
        extra={"signal": signal}
    )
    
    args = [
        "base", "+record-upsert",
        "--as", "bot",
        "--base-token", TRADING_HUB_TOKEN,
        "--table-id", SIGNALS_TABLE,
    ]
    
    try:
        result = _run_lark_cli(args, json.dumps(signal))
        logger.info("Trading signal written successfully", extra={"result": result})
        return result
    except BitableWriteError:
        raise
    except Exception as e:
        logger.exception("Unexpected error writing trading signal")
        raise BitableWriteError(f"Failed to write trading signal: {e}") from e


def write_performance_record(perf: dict[str, Any]) -> dict[str, Any]:
    """
    Write a performance record to the Trading Hub Bitable (Performance table).
    
    Args:
        perf: Performance record dict with fields:
            - Trades: Number of trades
            - Win Rate: 0.0-1.0 float
            - Signals Generated: Number of signals
            - Date: Timestamp (auto-filled if missing)
            - Daily PnL: Profit/loss amount
    
    Returns:
        Response from lark-cli with success status
        
    Raises:
        BitableWriteError: If write operation fails
    """
    # Ensure timestamp
    if "Date" not in perf:
        perf["Date"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    
    logger.info(
        "Writing performance record: %d trades, %.2f PnL",
        perf.get("Trades", 0),
        perf.get("Daily PnL", 0),
        extra={"perf": perf}
    )
    
    args = [
        "base", "+record-upsert",
        "--as", "bot",
        "--base-token", TRADING_HUB_TOKEN,
        "--table-id", PERFORMANCE_TABLE,
    ]
    
    try:
        result = _run_lark_cli(args, json.dumps(perf))
        logger.info("Performance record written successfully", extra={"result": result})
        return result
    except BitableWriteError:
        raise
    except Exception as e:
        logger.exception("Unexpected error writing performance record")
        raise BitableWriteError(f"Failed to write performance record: {e}") from e


if __name__ == "__main__":
    # Test mode
    import sys
    logging.basicConfig(level=logging.INFO)
    
    print("Testing Bitable Writer...")
    
    # Test research log write
    test_entry = {
        "文本": "Test: Autoresearch integration check",
        "单选": "TEST",
    }
    print(f"Writing research log: {test_entry}")
    try:
        result = write_research_log(test_entry)
        print(f"✓ Research log written: {result}")
    except BitableWriteError as e:
        print(f"✗ Research log failed: {e}")