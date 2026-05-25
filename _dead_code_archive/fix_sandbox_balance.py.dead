#!/usr/bin/env python3
"""Fix the AccountBalanceNegative crash in WhaleFollower."""

with open('strategies/whale_follower.py', 'r') as f:
    content = f.read()

# === Fix 1: Replace the balance check to account for sandbox fill price ===
old_balance = """        available = account.balance_free(USDC_e).as_double()
        if size_usd > available:
            self.log.info(
                f"Insufficient balance: need ${size_usd:,.2f}, "
                f"have ${available:,.2f} free, skipping"
            )
            return

        # Max open positions check"""

new_balance = """        available = account.balance_free(USDC_e).as_double()

        # Liquidity-based size adjustment (Track A)
        size_usd = self._adjust_size_for_liquidity(size_usd, inst_id)

        # Pre-compute qty to check sandbox auto-fill cost
        # Sandbox fills all market orders at $0.50 instead of actual market price
        SBDX_FILL_PRICE = 0.50
        qty_pre = instrument.make_qty(Decimal(str(size_usd / price)), round_down=True)
        effective_cost = max(size_usd, float(qty_pre.as_decimal()) * SBDX_FILL_PRICE)
        if effective_cost > available:
            self.log.info(
                f"Insufficient balance: effective cost ${effective_cost:,.2f} "
                f"(need ${size_usd:,.2f}, sandbox cost ${float(qty_pre.as_decimal()) * SBDX_FILL_PRICE:,.2f}) "
                f"exceeds available ${available:,.2f}, skipping"
            )
            return

        # Max open positions check"""

if old_balance not in content:
    # Try with different indentation
    lines = content.split('\n')
    for i, line in enumerate(lines):
        if 'if size_usd > available:' in line:
            indent = line[:len(line) - len(line.lstrip())]
            old_balance = f"""{indent}available = account.balance_free(USDC_e).as_double()
{indent}if size_usd > available:
{indent}    self.log.info(
{indent}        f"Insufficient balance: need ${size_usd:,.2f}, "
{indent}        f"have ${available:,.2f} free, skipping"
{indent}    )
{indent}    return
{indent}
{indent}# Max open positions check"""
            break
    if old_balance not in content:
        print("ERROR: Could not find balance check")
        print("Lines around match:")
        for j in range(max(0,i-2), min(len(lines), i+5)):
            print(f"  {j}: |{lines[j]}|")
        exit(1)

content = content.replace(old_balance, new_balance, 1)

# === Fix 2: Remove the now-duplicate liquidity adjustment ===
old_liquidity = """        # Liquidity-based size adjustment (Track A)
        size_usd = self._adjust_size_for_liquidity(size_usd, inst_id)

        qty = instrument.make_qty(Decimal(str(size_usd / price)), round_down=True)"""

# Find the actual indentation
if old_liquidity not in content:
    lines = content.split('\n')
    for i, line in enumerate(lines):
        if '# Liquidity-based size adjustment' in line:
            indent = line[:len(line) - len(line.lstrip())]
            old_liquidity = f"""{indent}# Liquidity-based size adjustment (Track A)
{indent}size_usd = self._adjust_size_for_liquidity(size_usd, inst_id)
{indent}
{indent}qty = instrument.make_qty(Decimal(str(size_usd / price)), round_down=True)"""
            break
    if old_liquidity not in content:
        print("ERROR: Could not find liquidity adjustment")
        exit(1)

new_qty_only = """        qty = instrument.make_qty(Decimal(str(size_usd / price)), round_down=True)"""
# Use same indent
indent = old_liquidity.split('\n')[0][:len(old_liquidity.split('\n')[0]) - len(old_liquidity.split('\n')[0].lstrip())]
new_qty_only = f"""{indent}qty = instrument.make_qty(Decimal(str(size_usd / price)), round_down=True)"""

content = content.replace(old_liquidity, new_qty_only, 1)

with open('strategies/whale_follower.py', 'w') as f:
    f.write(content)

print("PATCH APPLIED SUCCESSFULLY")
print("Changes made:")
print("  1. Moved _adjust_size_for_liquidity before balance check")
print("  2. Added pre-compute qty and check sandbox fill cost (0.50)")
print("  3. Removed duplicate liquidity adjustment")
