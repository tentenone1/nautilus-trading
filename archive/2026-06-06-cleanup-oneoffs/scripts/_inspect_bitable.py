#!/usr/bin/env python3
"""Inspect Bitable table schema and list records."""
import json, os, urllib.request, sys

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

def main():
    token = get_tenant_token()
    print(f"Token: {token[:20]}...")

    # Get table schema
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{TABLE_ID}/fields"
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        schema = json.loads(resp.read())
    print("=== SCHEMA ===")
    for field in schema.get("data", {}).get("items", []):
        print(f"  Field: {field['field_name']} (id={field['field_id']}, type={field['type']})")

    # List first 10 records with all fields
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{BASE_ID}/tables/{TABLE_ID}/records?page_size=20"
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        records = json.loads(resp.read())
    print("\n=== RECORDS ===")
    for item in records.get("data", {}).get("items", []):
        rid = item["record_id"]
        fields = item.get("fields", {})
        # Print first identifying field
        first_val = list(fields.values())[0] if fields else "EMPTY"
        print(f"  {rid}: {json.dumps(fields, ensure_ascii=False)[:200]}")

if __name__ == "__main__":
    main()
