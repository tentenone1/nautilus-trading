#!/usr/bin/env python3
"""Verify Bitable updates and compare with previous classifications."""
import json, os, urllib.request

APP_ID = os.environ.get("FEISHU_APP_ID", "cli_a94416fa03389cbd")
APP_SECRET = os.environ.get("FEISHU_APP_SECRET", "")
BASE_ID = "Jwr7b4Rf2a1EsfsqvwZcFJoXnVf"
TABLE_ID = "tblSM2BBBGJbZGO3"
PARSED_PATH = "/Users/tentenone/workspace/nautilus-trading/research/jailbreak_deep_parsed.json"

def get_tenant_token():
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    payload = json.dumps({"app_id": APP_ID, "app_secret": APP_SECRET}).encode()
    req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read())["tenant_access_token"]

def main():
    token = get_tenant_token()
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{TABLE_ID}/records?page_size=500"
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        records = json.loads(resp.read())

    items = records.get("data", {}).get("items", [])
    print(f"=== BITABLE VERIFICATION ({len(items)} records) ===\n")
    for item in items:
        fields = item["fields"]
        name = fields.get("Whale Name", "???")
        verdict = fields.get("Jailbreak Verdict", "")
        conf = fields.get("Confidence", "?")
        # Extract action from verdict
        action = verdict.split(".")[0] if verdict else "?"
        print(f"  {name:25s} | {action:6s} | conf={conf:>4s} | {verdict[:60]}")

    # Load new parsed results for comparison
    with open(PARSED_PATH) as f:
        new_data = json.load(f)
    
    print(f"\n=== UPDATED DATA (from Qwen analysis) ===")
    print(f"Generated: {new_data.get('_meta', {}).get('generated', 'N/A')}")
    print(f"Model: {new_data.get('_meta', {}).get('model', 'N/A')}")
    
    ca = new_data.get("cross_analysis", {})
    print(f"\nBest COPY pair: {ca.get('best_copy_pair')}")
    print(f"Most FADE:       {ca.get('most_fade')}")
    print(f"Tony wallet:     {ca.get('tony_wallet_status')}")
    print(f"\nCoordination: {ca.get('coordination_findings')}")
    print(f"\nRecommendation: {ca.get('overall_recommendation')}")

if __name__ == "__main__":
    main()
