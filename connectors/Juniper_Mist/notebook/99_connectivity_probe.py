# Databricks notebook source
# MAGIC %md
# MAGIC # jetstream-juniper-mist-connector — Connectivity Probe
# MAGIC
# MAGIC Read-only API smoke test. Verifies:
# MAGIC   1. The API token resolves from the secret scope.
# MAGIC   2. `GET /api/v1/self` succeeds and shows the token's privilege set.
# MAGIC   3. `GET /api/v1/orgs/{org_id}` succeeds and returns the org name.
# MAGIC   4. `GET /api/v1/orgs/{org_id}/sites` returns the site count.
# MAGIC
# MAGIC **Does not write to any destination or control-plane table.**

# COMMAND ----------

# MAGIC %pip install -q requests python-dotenv

# COMMAND ----------

import os
import sys

_HERE = os.path.dirname(os.path.abspath("__file__"))
sys.path.insert(0, os.path.abspath(os.path.join(_HERE, "..")))

# COMMAND ----------

dbutils.widgets.text("secret_scope", "", "secret_scope")
dbutils.widgets.text("juniper_mist_org_id", "", "juniper_mist_org_id")
dbutils.widgets.text("juniper_mist_base_url", "https://api.mist.com/api/v1", "juniper_mist_base_url")

# COMMAND ----------

import requests

secret_scope = dbutils.widgets.get("secret_scope")
org_id       = dbutils.widgets.get("juniper_mist_org_id")
base_url     = dbutils.widgets.get("juniper_mist_base_url").rstrip("/")

token = dbutils.secrets.get(scope=secret_scope, key="juniper_mist_token")
headers = {"Authorization": f"Token {token}"}

def probe(label, url, optional=False):
    try:
        r = requests.get(url, headers=headers, timeout=30)
        if r.status_code == 200:
            print(f"  ✓ {label}: HTTP 200")
            return r.json()
        else:
            msg = f"HTTP {r.status_code}: {r.text[:200]}"
            if optional:
                print(f"  ⚠ {label}: {msg} (optional — continuing)")
            else:
                raise RuntimeError(f"{label} failed: {msg}")
    except requests.RequestException as exc:
        msg = str(exc)
        if optional:
            print(f"  ⚠ {label}: {msg} (optional — continuing)")
        else:
            raise

print("=" * 60)
print("Juniper Mist API — Connectivity Probe")
print("=" * 60)
print(f"Base URL : {base_url}")
print(f"Org ID   : {org_id}")
print()

# Step 1: token identity
me = probe("GET /self", f"{base_url}/self")
if me:
    print(f"    Token name : {me.get('name', '(unknown)')}")
    for priv in (me.get("privileges") or []):
        print(f"    Privilege  : scope={priv.get('scope')} role={priv.get('role')} org={priv.get('org_id')}")
print()

# Step 2: org
org = probe("GET /orgs/{org_id}", f"{base_url}/orgs/{org_id}")
if org:
    print(f"    Org name   : {org.get('name', '(unknown)')}")
    print(f"    Modified   : {org.get('modified_time', 'N/A')}")
print()

# Step 3: sites count
import json
sites_resp = probe("GET /orgs/{org_id}/sites?page=1&limit=10", f"{base_url}/orgs/{org_id}/sites?page=1&limit=10")
if sites_resp is not None:
    count = len(sites_resp) if isinstance(sites_resp, list) else "?"
    print(f"    First-page sites returned: {count}")
print()

# Step 4: inventory count
inv_resp = probe("GET /orgs/{org_id}/inventory?page=1&limit=10", f"{base_url}/orgs/{org_id}/inventory?page=1&limit=10")
if inv_resp is not None:
    count = len(inv_resp) if isinstance(inv_resp, list) else "?"
    print(f"    First-page inventory returned: {count}")
print()

print("=" * 60)
print("All connectivity checks passed.")
print("=" * 60)
