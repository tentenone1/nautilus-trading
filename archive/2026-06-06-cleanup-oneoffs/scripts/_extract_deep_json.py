#!/usr/bin/env python3
"""Extract JSON from jailbreak_deep_analysis.json LLM response."""
import json, re, sys

with open('/Users/tentenone/workspace/nautilus-trading/research/jailbreak_deep_analysis.json') as f:
    data = json.load(f)

response = data['llm_response']

# Find JSON after closing think tag
if '</think>' in response:
    after_think = response.split('</think>')[1].strip()
else:
    after_think = response

# Try to find ```json block
json_match = re.search(r'```json\n(.*?)\n```', after_think, re.DOTALL)
if json_match:
    json_str = json_match.group(1).strip()
else:
    # Try to find any ``` block
    json_match = re.search(r'```\n?(.*?)\n?```', after_think, re.DOTALL)
    if json_match:
        json_str = json_match.group(1).strip()
    else:
        # Try to find raw JSON object at top level
        json_match = re.search(r'(\{.*\})', after_think, re.DOTALL)
        if json_match:
            json_str = json_match.group(1).strip()
        else:
            json_str = after_think

try:
    parsed = json.loads(json_str)
    with open('/Users/tentenone/workspace/nautilus-trading/research/jailbreak_deep_parsed.json', 'w') as f:
        json.dump(parsed, f, indent=2)
    print("PARSED SUCCESSFULLY")
    print(json.dumps(parsed, indent=2))
except Exception as e:
    print(f"PARSE ERROR: {e}")
    # Save raw for inspection
    with open('/Users/tentenone/workspace/nautilus-trading/research/jailbreak_raw_extracted.txt', 'w') as f:
        f.write(json_str)
    print(f"Saved raw extracted text ({len(json_str)} chars)")
    # Try to find any JSON in the response
    print("\n--- Response preview (last 2000 chars) ---")
    print(after_think[-2000:])
