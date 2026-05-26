import json, os, time

def main():
    base_dir = os.path.abspath(os.path.dirname(__file__) + '/..')
    intel_path = os.path.join(base_dir, 'research', 'sybil_intelligence.json')
    db_path = '/data/trades.db'
    # Load intelligence JSON
    with open(intel_path, 'r') as f:
        intel = json.load(f)
    # Collect signals from top-level 'signals' array
    signals = intel.get('signals', [])
    if not signals:
        print('No signals found to append.')
        return
    # Append each signal as a JSON line matching existing format
    with open(db_path, 'a') as f:
        for s in signals:
            f.write(json.dumps(s) + '\n')
    print(f'Appended {len(signals)} signals to {db_path}')

if __name__ == "__main__":
    main()