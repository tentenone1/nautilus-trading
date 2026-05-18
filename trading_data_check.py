import sqlite3
import json

# Check whale_discovery.db schema
conn = sqlite3.connect("/home/elon-1/workspace/nautilus-trading/data/whale_discovery.db")
c = conn.cursor()
c.execute("SELECT name FROM sqlite_master WHERE type='table'")
tables = c.fetchall()
print('=== whale_discovery.db tables ===')
for t in tables:
    print(t[0])
    c.execute("PRAGMA table_info(" + t[0] + ")")
    cols = c.fetchall()
    for col in cols:
        print("  " + col[1] + " (" + col[2] + ")")

# Get whale data from whale_discovery
try:
    c.execute('SELECT * FROM whales ORDER BY alpha_score DESC LIMIT 30')
    whales = c.fetchall()
    if whales:
        col_names = [desc[0] for desc in c.description]
        print('\n=== WHALES BY ALPHA (Top 30) ===')
        print('Columns:', col_names)
        for w in whales[:10]:
            print(w)
except Exception as e:
    print('whales table error: ' + str(e))

conn.close()

# Check trades.db
conn2 = sqlite3.connect("/home/elon-1/workspace/nautilus-trading/data/trades.db")
c2 = conn2.cursor()
c2.execute("SELECT name FROM sqlite_master WHERE type='table'")
tables2 = c2.fetchall()
print('\n=== trades.db tables ===')
for t in tables2:
    print(t[0])
    c2.execute("PRAGMA table_info(" + t[0] + ")")
    cols = c2.fetchall()
    for col in cols:
        print("  " + col[1] + " (" + col[2] + ")")

# Get recent trades
try:
    c2.execute('SELECT * FROM trades WHERE exit_time IS NOT NULL ORDER BY exit_time DESC LIMIT 20')
    trades = c2.fetchall()
    if trades:
        col_names = [desc[0] for desc in c2.description]
        print('\n=== RECENT CLOSED TRADES ===')
        print('Columns:', col_names)
        for t in trades[:5]:
            print(t)
    
    # Summary
    c2.execute('SELECT COUNT(*), SUM(CASE WHEN outcome="win" THEN 1 ELSE 0 END), SUM(pnl) FROM trades WHERE exit_time IS NOT NULL')
    stats = c2.fetchone()
    total = stats[0] or 0
    wins = stats[1] or 0
    pnl = stats[2] or 0
    print('\n=== SUMMARY ===')
    print('Closed trades: ' + str(total))
    print('Wins: ' + str(wins))
    if total > 0:
        print('Win Rate: ' + str(round(wins/total*100, 1)) + '%')
    print('Total PnL: $' + str(round(pnl, 2)))
except Exception as e:
    print('trades query error: ' + str(e))

conn2.close()

# Check whale_profiles.json
try:
    with open('research/whale_profiles.json', 'r') as f:
        profiles = json.load(f)
    print('\n=== whale_profiles.json (' + str(len(profiles)) + ' whales) ===')
    
    # Count by classification
    classifications = {}
    fade_count = 0
    for addr, p in profiles.items():
        cls = p.get('classification', 'unknown')
        classifications[cls] = classifications.get(cls, 0) + 1
        if p.get('should_fade'):
            fade_count += 1
    
    print('Classifications:')
    for cls, count in sorted(classifications.items(), key=lambda x: -x[1]):
        print('  ' + cls + ': ' + str(count))
    print('Whales marked for FADE: ' + str(fade_count))
    
    # Show sample losers
    losers = [(addr, p) for addr, p in profiles.items() if p.get('should_fade')][:5]
    print('\nSample whales marked for FADE:')
    for addr, p in losers:
        cls = p.get('classification', 'unknown')
        wr = p.get('win_rate', 0)
        print('  ' + addr[:20] + ': ' + cls + ' WR=' + str(wr) + '%')
except Exception as e:
    print('profiles error: ' + str(e))