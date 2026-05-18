import sqlite3

# Check trades.db
conn = sqlite3.connect('trades.db')
c = conn.cursor()

# Get schema
c.execute("SELECT name FROM sqlite_master WHERE type='table'")
tables = c.fetchall()
print('=== trades.db tables ===')
for t in tables:
    print(t[0])

# Get recent trades
c.execute('SELECT * FROM trades ORDER BY created_at DESC LIMIT 10')
trades = c.fetchall()
cols = [desc[0] for desc in c.description]
print()
print('Columns:', cols)
print()
print('Recent trades:')
for t in trades[:5]:
    print(t)

# Summary
try:
    c.execute('SELECT COUNT(*) FROM trades')
    total = c.fetchone()[0]
    
    c.execute('SELECT COUNT(*) FROM trades WHERE status="closed"')
    closed = c.fetchone()[0]
    
    c.execute('SELECT SUM(CASE WHEN outcome="win" THEN 1 ELSE 0 END), COUNT(*) FROM trades WHERE status="closed"')
    result = c.fetchone()
    wins = result[0] or 0
    closed_total = result[1] or 0
    
    c.execute('SELECT SUM(pnl) FROM trades WHERE status="closed"')
    pnl = c.fetchone()[0] or 0
    
    print()
    print('=== SUMMARY ===')
    print('Total trades:', total)
    print('Closed trades:', closed)
    print('Wins:', wins)
    if closed_total > 0:
        print('Win Rate:', round(wins/closed_total*100, 1), '%')
    print('Total PnL:', round(pnl, 2))
except Exception as e:
    print('Error:', e)

conn.close()