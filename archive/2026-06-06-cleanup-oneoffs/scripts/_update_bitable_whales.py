#!/usr/bin/env python3
"""Update Bitable with jailbreak whale analysis results."""
import json, os, urllib.request, sys, shutil
from datetime import datetime

BASE_DIR = "/Users/tentenone/workspace/nautilus-trading"
PARSED_PATH = os.path.join(BASE_DIR, "research", "jailbreak_deep_parsed.json")
PREVIOUS_PATH = os.path.join(BASE_DIR, "research", "jailbreak_deep_parsed.json.prev")

APP_ID = os.environ.get("FEISHU_APP_ID", "cli_a94416fa03389cbd")
APP_SECRET = os.environ.get("FEISHU_APP_SECRET", "")
BASE_ID = "Jwr7b4Rf2a1EsfsqvwZcFJoXnVf"
TABLE_ID = "tblSM2BBBGJbZGO3"

def get_tenant_token():
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    payload = json.dumps({"app_id": APP_ID, "app_secret": APP_SECRET}).encode()
    req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as resp:
        data = json.loads(resp.read())
    return data.get("tenant_access_token", "")

def api_get(url, token):
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())

def api_put(url, token, data):
    payload = json.dumps(data).encode()
    req = urllib.request.Request(url, data=payload, headers={
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    })
    req.method = "PUT"
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        return {"error": e.code, "body": e.read().decode()}

def main():
    token = get_tenant_token()
    print(f"Got token: {token[:20]}...")

    # Load new parsed results
    with open(PARSED_PATH) as f:
        new_data = json.load(f)

    # Load previous for comparison
    prev_data = None
    prev_actions = {}
    if os.path.exists(PREVIOUS_PATH):
        with open(PREVIOUS_PATH) as f:
            prev_data = json.load(f)
        for w in prev_data.get("whales", []):
            prev_actions[w["name"].lower().strip()] = w["action"]
        print(f"Loaded previous run with {len(prev_actions)} whale classifications")

    # Get Type field options (fldUqEH5WL)
    fields_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{TABLE_ID}/fields"
    fields = api_get(fields_url, token)
    type_options = {}
    for field in fields.get("data", {}).get("items", []):
        if field["field_id"] == "fldUqEH5WL":
            for opt in field.get("property", {}).get("options", []):
                type_options[opt["name"]] = opt["id"]
            break
    print(f"Type options: {type_options}")

    # List existing records
    records_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{TABLE_ID}/records?page_size=500"
    records_data = api_get(records_url, token)
    existing = records_data.get("data", {}).get("items", [])
    print(f"Found {len(existing)} records in Bitable")

    # Build name -> record_id mapping
    name_to_record = {}
    for rec in existing:
        fields = rec.get("fields", {})
        name = fields.get("Whale Name", "")
        if name:
            name_to_record[name.lower().strip()] = rec["record_id"]

    print(f"Known whale names: {list(name_to_record.keys())}")

    # Build new whale data
    new_whales = {}
    for w in new_data.get("whales", []):
        name = w["name"].lower().strip()
        new_whales[name] = w

    # Update records
    updates = 0
    classification_changes = []
    for name_lower, w in new_whales.items():
        if name_lower in name_to_record:
            record_id = name_to_record[name_lower]

            # Build verdict string
            verdict = f"{w['action']}. {w['style']} {w['skill'].title()}. conf={w['confidence']:.2f}. (5900X jailbreak)"

            # Get type option ID
            type_id = type_options.get(w["action"], w["action"])

            fields = {
                "Type": type_id,
                "Jailbreak Verdict": verdict,
                "Confidence": w["confidence"],
                "Last Updated": int(datetime.now().timestamp()),
            }
            result = api_put(
                f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{TABLE_ID}/records/{record_id}",
                token,
                {"fields": fields}
            )
            if result.get("code") == 0:
                updates += 1
                prev_action = prev_actions.get(name_lower)
                if prev_action and prev_action != w["action"]:
                    classification_changes.append(f"{w['name']}: {prev_action} \u2192 {w['action']}")
                print(f"  OK {w['name']}: {w['action']} conf={w['confidence']}")
            else:
                print(f"  FAIL {w['name']}: {result}")
        else:
            print(f"  SKIP {w['name']}: no matching Bitable record found")

    # Save current as previous for next comparison
    shutil.copy(PARSED_PATH, PREVIOUS_PATH)

    # Print summary
    print(f"\n{'='*60}")
    print(f"BITABLE UPDATE: {updates} records updated")
    if classification_changes:
        print(f"\n!! CLASSIFICATION CHANGES ({len(classification_changes)}):")
        for c in classification_changes:
            print(f"   {c}")
    else:
        print(f"\nNo classification changes detected")

    # Group by action for report
    print(f"\n--- COPY WHALES ---")
    for w in new_data.get("whales", []):
        if w["action"] == "COPY":
            prev = prev_actions.get(w["name"].lower().strip(), "NEW")
            prefix = " ** NEW" if prev == "NEW" else (" !! CHANGED" if prev != "COPY" else "")
            print(f"  {w['name']:25s} conf={w['confidence']:.2f}{prefix}")

    print(f"\n--- FADE WHALES ---")
    for w in new_data.get("whales", []):
        if w["action"] == "FADE":
            prev = prev_actions.get(w["name"].lower().strip(), "NEW")
            prefix = " ** NEW" if prev == "NEW" else (" !! CHANGED" if prev != "FADE" else "")
            print(f"  {w['name']:25s} conf={w['confidence']:.2f}{prefix}")

    print(f"\n--- WATCH WHALES ---")
    for w in new_data.get("whales", []):
        if w["action"] == "WATCH":
            prev = prev_actions.get(w["name"].lower().strip(), "NEW")
            prefix = " ** NEW" if prev == "NEW" else (" !! CHANGED" if prev != "WATCH" else "")
            print(f"  {w['name']:25s} conf={w['confidence']:.2f}{prefix}")

    ca = new_data.get("cross_analysis", {})
    print(f"\n--- CROSS ANALYSIS ---")
    print(f"  Best COPY pair:      {ca.get('best_copy_pair')}")
    print(f"  Most FADE:           {ca.get('most_fade')}")
    print(f"  Tony wallet:         {ca.get('tony_wallet_status')}")
    print(f"  Coordination:        {ca.get('coordination_findings')}")
    print(f"  Recommendation:      {ca.get('overall_recommendation')}")

if __name__ == "__main__":
    main()
