#!/usr/bin/env python3
"""Orderbook Adapter — Pulls on-chain fill data from Polygon and writes to PostgreSQL.

Uses free Polygon RPC endpoints to query OrderFilled events from Polymarket's
CTF Exchange and Neg Risk Exchange contracts. Writes to the same PostgreSQL
schema as polymarket-orderbook-substreams, enabling the nautilus trading system
to query real-time fill data.

Usage:
    python3 scripts/orderbook_adapter.py [--once] [--poll-interval 15]
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("orderbook_adapter")

# ── Configuration ─────────────────────────────────────────────────────────

CTF_EXCHANGE_V1 = "0x4bFb41D5b3570DeFd03C39A9a4D8dE6bD8B8982E"
NEG_RISK_EXCHANGE_V1 = "0xC5d563A36AE78145C45a50134d48A1215220f80a"
V2_START_BLOCK = 84_902_353

RPC_URLS = [
    "https://polygon-bor-rpc.publicnode.com",
    "https://polygon.drpc.org",
    "https://1rpc.io/matic",
]

PG_DSN = "postgresql://polymarket:polymarket@localhost:5432/polymarket_orderbook"
DEFAULT_POLL_INTERVAL = 15
BLOCKS_PER_POLL = 500

STATE_FILE = Path("/home/elon-1/workspace/nautilus-trading/data/orderbook_adapter_state.json")

# ── RPC Calls ──────────────────────────────────────────────────────────────

def rpc_call(method: str, params: list, rpc_url: str) -> dict | None:
    payload = json.dumps({"jsonrpc": "2.0", "method": method, "params": params, "id": 1}).encode()
    try:
        req = urllib.request.Request(rpc_url, data=payload, headers={"Content-Type": "application/json", "User-Agent": "Mozilla/5.0", "Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read())
            if "error" in data:
                return None
            return data.get("result")
    except Exception as e:
        logger.debug(f"RPC failed ({rpc_url}): {e}")
        return None

def try_rpc(method: str, params: list) -> dict | None:
    for rpc in RPC_URLS:
        result = rpc_call(method, params, rpc)
        if result is not None:
            return result
    return None

def get_latest_block() -> int | None:
    result = try_rpc("eth_blockNumber", [])
    if result:
        return int(result, 16)
    return None

def get_logs(from_block: int, to_block: int, address: str) -> list[dict]:
    params = [{
        "fromBlock": hex(from_block),
        "toBlock": hex(to_block),
        "address": address,
    }]
    logs = try_rpc("eth_getLogs", params)
    if isinstance(logs, list):
        return logs
    return []

# ── Fill Parsing ────────────────────────────────────────────────────────────

def parse_fill_log(log: dict, contract_addr: str) -> dict | None:
    try:
        topics = log.get("topics", [])
        data_hex = log.get("data", "0x")
        tx_hash = log.get("transactionHash", "")
        block_num = int(log.get("blockNumber", "0x0"), 16)
        log_index = int(log.get("logIndex", "0x0"), 16)

        if len(topics) < 4:
            return None

        order_hash = topics[1]
        maker = "0x" + topics[2][-40:]
        taker = "0x" + topics[3][-40:]

        is_v2 = block_num >= V2_START_BLOCK
        data_bytes = bytes.fromhex(data_hex[2:]) if data_hex.startswith("0x") else bytes.fromhex(data_hex)

        if not is_v2:
            if len(data_bytes) < 160:
                return None
            maker_asset_id = int.from_bytes(data_bytes[0:32], "big")
            taker_asset_id = int.from_bytes(data_bytes[32:64], "big")
            maker_amount = int.from_bytes(data_bytes[64:96], "big")
            taker_amount = int.from_bytes(data_bytes[96:128], "big")
            fee = int.from_bytes(data_bytes[128:160], "big")
            price = taker_amount / maker_amount if maker_amount > 0 else 0
            return {
                "id": f"{tx_hash}-{log_index}",
                "transaction_hash": tx_hash, "order_hash": order_hash,
                "maker": maker, "taker": taker,
                "maker_asset_id": str(maker_asset_id), "taker_asset_id": str(taker_asset_id),
                "maker_amount_filled": maker_amount, "taker_amount_filled": taker_amount,
                "fee": fee, "side": "buy", "price": price,
                "block_number": block_num, "exchange_version": "v1",
                "token_id": str(maker_asset_id), "side_raw": 0,
                "builder": None, "metadata": None,
            }
        else:
            if len(data_bytes) < 224:
                return None
            side_raw = int.from_bytes(data_bytes[0:32], "big")
            token_id = int.from_bytes(data_bytes[32:64], "big")
            maker_amount = int.from_bytes(data_bytes[64:96], "big")
            taker_amount = int.from_bytes(data_bytes[96:128], "big")
            fee = int.from_bytes(data_bytes[128:160], "big")
            builder = "0x" + data_bytes[160:192].hex()
            metadata_val = "0x" + data_bytes[192:224].hex()
            side = "buy" if side_raw == 1 else "sell"
            price = taker_amount / maker_amount if maker_amount > 0 else 0
            return {
                "id": f"{tx_hash}-{log_index}",
                "transaction_hash": tx_hash, "order_hash": order_hash,
                "maker": maker, "taker": taker,
                "maker_asset_id": str(token_id), "taker_asset_id": str(token_id),
                "maker_amount_filled": maker_amount, "taker_amount_filled": taker_amount,
                "fee": fee, "side": side, "price": price,
                "block_number": block_num, "exchange_version": "v2",
                "token_id": str(token_id), "side_raw": side_raw,
                "builder": builder, "metadata": metadata_val,
            }
    except Exception as e:
        logger.debug(f"Parse error: {e}")
        return None

# ── Database ────────────────────────────────────────────────────────────────

def get_pg_connection():
    try:
        import psycopg2
        conn = psycopg2.connect(PG_DSN)
        conn.autocommit = True
        return conn
    except ImportError:
        logger.error("psycopg2 not installed")
        return None

def insert_fills(conn, fills: list[dict]) -> int:
    if not fills:
        return 0
    cur = conn.cursor()
    inserted = 0
    for f in fills:
        try:
            cur.execute("""
                INSERT INTO order_fills (id, transaction_hash, order_hash, maker, taker,
                    maker_asset_id, taker_asset_id, maker_amount_filled, taker_amount_filled,
                    fee, side, price, block_number, exchange_version, token_id, side_raw, builder, metadata)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING
            """, (f["id"], f["transaction_hash"], f["order_hash"], f["maker"], f["taker"],
                    f["maker_asset_id"], f["taker_asset_id"], f["maker_amount_filled"],
                    f["taker_amount_filled"], f["fee"], f["side"], f["price"],
                    f["block_number"], f["exchange_version"], f["token_id"],
                    f["side_raw"], f["builder"], f["metadata"]))
            if cur.rowcount > 0:
                inserted += 1
        except Exception as e:
            logger.debug(f"Insert skip: {e}")
    cur.close()
    return inserted

def update_aggregates(conn, fills: list[dict]) -> None:
    if not fills:
        return
    cur = conn.cursor()
    market_stats = defaultdict(lambda: {"trades": 0, "buys": 0, "sells": 0, "volume": 0, "fees": 0, "prices": []})
    for f in fills:
        tid = f.get("token_id", f.get("maker_asset_id", ""))
        s = market_stats[tid]
        s["trades"] += 1
        s["buys"] += 1 if f["side"] == "buy" else 0
        s["sells"] += 1 if f["side"] == "sell" else 0
        s["volume"] += f["taker_amount_filled"]
        s["fees"] += f["fee"]
        if f["price"] > 0:
            s["prices"].append(f["price"])

    for tid, s in market_stats.items():
        avg_price = sum(s["prices"]) / len(s["prices"]) if s["prices"] else 0
        avg_size = s["volume"] // max(s["trades"], 1)
        blk = fills[-1]["block_number"]
        try:
            cur.execute("""
                INSERT INTO market_orderbooks (id, condition_id, trades_quantity, buys_quantity,
                    sells_quantity, collateral_volume, average_trade_size, total_fees, mid_price, last_updated_block)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    trades_quantity = market_orderbooks.trades_quantity + EXCLUDED.trades_quantity,
                    buys_quantity = market_orderbooks.buys_quantity + EXCLUDED.buys_quantity,
                    sells_quantity = market_orderbooks.sells_quantity + EXCLUDED.sells_quantity,
                    collateral_volume = market_orderbooks.collateral_volume + EXCLUDED.collateral_volume,
                    total_fees = market_orderbooks.total_fees + EXCLUDED.total_fees,
                    mid_price = EXCLUDED.mid_price,
                    last_updated_block = EXCLUDED.last_updated_block
            """, (tid, tid, s["trades"], s["buys"], s["sells"], s["volume"], avg_size, s["fees"], avg_price, blk))
        except Exception:
            pass

    trader_stats = defaultdict(lambda: {"trades": 0, "volume": 0, "fees": 0})
    for f in fills:
        for addr in [f["maker"], f["taker"]]:
            trader_stats[addr]["trades"] += 1
            trader_stats[addr]["volume"] += f["taker_amount_filled"]
            trader_stats[addr]["fees"] += f["fee"]
    for addr, s in trader_stats.items():
        try:
            cur.execute("""
                INSERT INTO trader_accounts (id, trades_quantity, total_volume, total_fees, is_active, trader_type)
                VALUES (%s, %s, %s, %s, true, 'unknown')
                ON CONFLICT (id) DO UPDATE SET
                    trades_quantity = trader_accounts.trades_quantity + EXCLUDED.trades_quantity,
                    total_volume = trader_accounts.total_volume + EXCLUDED.total_volume,
                    total_fees = trader_accounts.total_fees + EXCLUDED.total_fees,
                    is_active = true
            """, (addr, s["trades"], s["volume"], s["fees"]))
        except Exception:
            pass
    cur.close()

# ── State ──────────────────────────────────────────────────────────────────

def load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except Exception:
            pass
    return {"last_block": 0, "total_fills": 0}

def save_state(state: dict) -> None:
    STATE_FILE.write_text(json.dumps(state, indent=2))

# ── Main ────────────────────────────────────────────────────────────────────

def run_once(conn) -> int:
    state = load_state()
    latest_block = get_latest_block()
    if latest_block is None:
        logger.error("Could not get latest block")
        return 0

    last_block = state.get("last_block", 0)
    if last_block == 0:
        last_block = latest_block - 240
        logger.info(f"No saved state, starting from block {last_block}")

    if latest_block <= last_block:
        return 0

    total_fills = 0
    from_block = last_block + 1
    while from_block <= latest_block:
        to_block = min(from_block + BLOCKS_PER_POLL - 1, latest_block)
        for addr in [CTF_EXCHANGE_V1, NEG_RISK_EXCHANGE_V1]:
            logs = get_logs(from_block, to_block, addr)
            if logs:
                fills = [f for l in logs if (f := parse_fill_log(l, addr)) is not None]
                if fills:
                    inserted = insert_fills(conn, fills)
                    update_aggregates(conn, fills)
                    total_fills += inserted
                    logger.info(f"Blocks {from_block}-{to_block} ({addr[:10]}...): {len(fills)} fills, {inserted} inserted")
        from_block = to_block + 1

    state["last_block"] = latest_block
    state["total_fills"] = state.get("total_fills", 0) + total_fills
    state["last_updated"] = datetime.now(timezone.utc).isoformat()
    save_state(state)
    return total_fills

def main():
    parser = argparse.ArgumentParser(description="Polymarket Orderbook Adapter")
    parser.add_argument("--once", action="store_true", help="Run once and exit")
    parser.add_argument("--poll-interval", type=int, default=DEFAULT_POLL_INTERVAL)
    parser.add_argument("--from-block", type=int, default=0)
    args = parser.parse_args()

    logger.info("=== Polymarket Orderbook Adapter ===")
    conn = get_pg_connection()
    if conn is None:
        sys.exit(1)
    logger.info("Connected to PostgreSQL")

    if args.from_block > 0:
        state = load_state()
        state["last_block"] = args.from_block
        save_state(state)
        logger.info(f"Set start block to {args.from_block}")

    if args.once:
        fills = run_once(conn)
        logger.info(f"Single run: {fills} fills")
    else:
        logger.info(f"Starting daemon (poll: {args.poll_interval}s)")
        while True:
            try:
                run_once(conn)
            except Exception as e:
                logger.error(f"Error: {e}")
            time.sleep(args.poll_interval)

if __name__ == "__main__":
    main()
