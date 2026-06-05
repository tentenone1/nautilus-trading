#!/usr/bin/env python3
"""Update Whale Analysis Bitable with current jailbreak deep analysis results."""
import json, os
from datetime import datetime, timezone
from urllib.request import Request, urlopen

ENV_PATH = "/opt/data/.env"
env_vars = {}
if os.path.exists(ENV_PATH):
    for line in open(ENV_PATH):
        if "=" in line and not line.startswith("#"):
            k, _, v = line.partition("=")
            env_vars[k.strip()] = v.strip()

APP_ID = env_vars.get("FEISHU_APP_ID", "")
APP_SECRET = env_vars.get("FEISHU_APP_SECRET", "")

BASE_TOKEN = "Jwr7b4Rf2a1EsfsqvwZcFJoXnVf"
TABLE_ID = "tblSM2BBBGJbZGO3"

PARSED_PATH = "/home/elon-1/workspace/nautilus-trading/research/jailbreak_deep_parsed.json"

def get_token():
    resp = json.loads(urlopen(Request(
        "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal",
        data=json.dumps({"app_id": APP_ID, "app_secret": APP_SECRET}).encode(),
        headers={"Content-Type": "application/json"}
    )).read())
    return resp["tenant_access_token"]

def update_record(token, record_id, fields):
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    payload = json.dumps({"fields": fields}).encode()
    req = Request(
        f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_TOKEN}/tables/{TABLE_ID}/records/{record_id}",
        data=payload, headers=headers, method="PUT"
    )
    resp = json.loads(urlopen(req).read())
    return resp.get("code") == 0, resp

def build_verdict(whale):
    action = whale["action"]
    style = whale["style"]
    skill = whale["skill"]
    conf = whale["confidence"]
    return f"{action}. {style} {skill.title()}. conf={conf:.2f}. (5900X jailbreak)"

# Load parsed analysis
with open(PARSED_PATH) as f:
    parsed = json.load(f)

whales = {w["name"]: w for w in parsed["whales"]}

# record_id -> whale name mapping (verified from Bitable)
RECORD_MAP = {
    "recviQsUP5D7Ho": "RJW1",
    "recviQsUP5Yv1e": "surfandturf",
    "recviQsUP57O9t": "matanovik",
    "recviQsUP5At6o": "p150-0xba389f",
    "recviQsUP5oVu9": "pilotbaby",
    "recviQsUP5RCBO": "asdfjh",
    "recviQsUP5E86d": "SMCAOMCRL",
    "recviQsUP5s31U": "benwyatt",
    "recviQsUP5FmES": "JPMorgan101",
    "recviQsUP5306z": "bossoskil1",
    "recviQsUP5MOQY": "trade-via-Gravia",
    "recviQsUP5hEhM": "Countryside",
}

token = get_token()
now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

success = 0
fail = 0
for record_id, name in RECORD_MAP.items():
    whale = whales.get(name)
    if not whale:
        print(f"  ⚠️  {name}: not found in parsed data, skipping")
        continue
    
    verdict = build_verdict(whale)
    fields = {
        "Jailbreak Verdict": verdict,
        "Confidence": whale["confidence"],
        "Last Updated": now_ms,
    }
    ok, resp = update_record(token, record_id, fields)
    if ok:
        success += 1
        action = whale["action"]
        print(f"  ✅ {name:25s} | {action:6s} | conf={whale['confidence']:.2f}")
    else:
        fail += 1
        err = resp.get("msg", "unknown")
        print(f"  ❌ {name:25s} FAILED: {err}")

print(f"\nSummary: {success} updated, {fail} failed")
