# Databricks notebook source
# MAGIC %md
# MAGIC # Zoom API Probe — Connectivity & Coverage Test
# MAGIC
# MAGIC **Purpose.** Validate every Zoom REST endpoint the connector will use, directly
# MAGIC from the Databricks egress path. Use this **before** running the full connector
# MAGIC to confirm: secret scope is wired, network egress reaches `api.zoom.us`, OAuth
# MAGIC token works, every required scope is granted, and each endpoint returns data.
# MAGIC
# MAGIC This is a **throwaway probe notebook** — does **not** call the connector code
# MAGIC and does **not** write to Unity Catalog. Just `requests` calls with pretty-printed
# MAGIC response samples.
# MAGIC
# MAGIC **Widgets:**
# MAGIC - `secret_scope` — Databricks secret scope name (default `jetstream_zoom`)
# MAGIC - `sample_user_id` — optional Zoom user ID to use for per-user calls (auto-picks first user if blank)
# MAGIC - `from_date` / `to_date` — window for past-meeting / metrics / report endpoints (default = yesterday → today)

# COMMAND ----------

# MAGIC %pip install -q requests

# COMMAND ----------

dbutils.widgets.text("secret_scope", "jetstream_zoom", "Secret scope")
dbutils.widgets.text("sample_user_id", "", "Sample user id (blank = auto)")
dbutils.widgets.text("from_date", "", "from (YYYY-MM-DD, blank = yesterday)")
dbutils.widgets.text("to_date", "", "to   (YYYY-MM-DD, blank = today)")

SECRET_SCOPE = dbutils.widgets.get("secret_scope").strip()
SAMPLE_USER_ID = dbutils.widgets.get("sample_user_id").strip() or None

from datetime import datetime, timedelta, timezone

_today = datetime.now(timezone.utc).date()
FROM_DATE = dbutils.widgets.get("from_date").strip() or (_today - timedelta(days=1)).isoformat()
TO_DATE = dbutils.widgets.get("to_date").strip() or _today.isoformat()

print(f"scope={SECRET_SCOPE}  from={FROM_DATE}  to={TO_DATE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 — Load credentials and fetch token
# MAGIC
# MAGIC Secrets expected in the named scope (see the README and `connectors/zoom/parameters.yaml`):
# MAGIC `zoom_account_id`, `zoom_client_id`, `zoom_client_secret`.

# COMMAND ----------

import json
import urllib.parse
from base64 import b64encode

import requests

API_BASE = "https://api.zoom.us/v2"
TOKEN_URL = "https://zoom.us/oauth/token"

account_id = dbutils.secrets.get(scope=SECRET_SCOPE, key="zoom_account_id")
client_id = dbutils.secrets.get(scope=SECRET_SCOPE, key="zoom_client_id")
client_secret = dbutils.secrets.get(scope=SECRET_SCOPE, key="zoom_client_secret")

basic = b64encode(f"{client_id}:{client_secret}".encode()).decode()
resp = requests.post(
    TOKEN_URL,
    params={"grant_type": "account_credentials", "account_id": account_id},
    headers={"Authorization": f"Basic {basic}"},
    timeout=30,
)
resp.raise_for_status()
tok = resp.json()
TOKEN = tok["access_token"]
GRANTED_SCOPES = set(tok.get("scope", "").split())

print(f"token type:  {tok.get('token_type')}")
print(f"expires_in:  {tok.get('expires_in')}s")
print(f"granted scopes ({len(GRANTED_SCOPES)}):")
for s in sorted(GRANTED_SCOPES):
    print(f"  - {s}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 — Scope coverage check
# MAGIC
# MAGIC Compare granted scopes against the set the connector needs. Any ❌ row must be
# MAGIC requested from the Zoom team before the corresponding endpoint will work.

# COMMAND ----------

REQUIRED_SCOPES = {
    "user:read:list_users:admin":                            "List users",
    "user:read:user:admin":                                   "User detail",
    "zoom_rooms:read:list_rooms:admin":                       "List Zoom Rooms",
    "zoom_rooms:read:room:admin":                             "Room detail (capacity)",
    "zoom_rooms:read:list_locations:admin":                   "Room location hierarchy",
    "zoom_rooms:read:list_devices:admin":                     "Room hardware inventory",
    "meeting:read:list_meetings:admin":                       "Per-user meetings (all types)",
    "meeting:read:past_meeting:admin":                        "Past meeting summary",
    "meeting:read:list_past_participants:admin":              "Past meeting participants",
    "group:read:list_groups:admin":                           "Groups list",
    "group:read:group:admin":                                 "Group detail (name lookup)",
    "cloud_recording:read:list_user_recordings:admin":        "Per-user recordings (optional)",
    "cloud_recording:read:list_account_recordings:admin":     "Account-wide recordings (optional)",
    "dashboard:read:list_meetings:admin":                     "Dashboard meetings (rich telemetry)",
    "dashboard:read:list_meeting_participants:admin":         "Dashboard participants (device + in_room)",
    "report:read:list_meeting_participants:admin":            "Report participants (alt path)",
}

# Also accept classic equivalents (a granted classic scope implies the granular set)
CLASSIC_EQUIVALENT = {
    "user:read:admin":               {"user:read:list_users:admin", "user:read:user:admin"},
    "room:read:admin":               {"zoom_rooms:read:list_rooms:admin", "zoom_rooms:read:room:admin",
                                      "zoom_rooms:read:list_locations:admin", "zoom_rooms:read:list_devices:admin"},
    "meeting:read:admin":            {"meeting:read:list_meetings:admin", "meeting:read:past_meeting:admin",
                                      "meeting:read:list_past_participants:admin"},
    "group:read:admin":              {"group:read:list_groups:admin", "group:read:group:admin"},
    "recording:read:admin":          {"cloud_recording:read:list_user_recordings:admin",
                                      "cloud_recording:read:list_account_recordings:admin"},
    "dashboard_meetings:read:admin": {"dashboard:read:list_meetings:admin",
                                      "dashboard:read:list_meeting_participants:admin"},
    "report:read:admin":             {"report:read:list_meeting_participants:admin"},
}

effective = set(GRANTED_SCOPES)
for classic, granular_set in CLASSIC_EQUIVALENT.items():
    if classic in GRANTED_SCOPES:
        effective |= granular_set

print(f"{'STATUS':6}  {'SCOPE':55}  PURPOSE")
print("-" * 110)
for scope, purpose in REQUIRED_SCOPES.items():
    ok = "✅" if scope in effective else "❌"
    print(f"{ok:6}  {scope:55}  {purpose}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 — Endpoint probes
# MAGIC
# MAGIC Helper that prints status, rate-limit headers, and a small JSON sample.

# COMMAND ----------

HEADERS = {"Authorization": f"Bearer {TOKEN}", "Accept": "application/json"}

def probe(label: str, path: str, params: dict | None = None, *, show: int = 1):
    url = path if path.startswith("http") else f"{API_BASE}{path}"
    r = requests.get(url, headers=HEADERS, params=params or {}, timeout=30)
    rl_cat = r.headers.get("X-RateLimit-Category", "-")
    rl_type = r.headers.get("X-RateLimit-Type", "-")
    rl_rem = r.headers.get("X-RateLimit-Remaining", "-")
    print(f"[{r.status_code}] {label:35} cat={rl_cat:18} type={rl_type:6} remaining={rl_rem}")
    print(f"        {r.request.method} {r.url}")
    try:
        body = r.json()
    except Exception:
        print(f"        non-json body: {r.text[:200]}")
        return None
    if r.status_code >= 400:
        print(f"        ERROR  body: {json.dumps(body, indent=2)[:400]}")
        return None
    # Show a sample
    sample = body
    for k in ("users", "rooms", "locations", "devices", "meetings", "participants",
              "recordings", "groups"):
        if isinstance(body, dict) and k in body and isinstance(body[k], list):
            sample = {k: body[k][:show], "total_records": body.get("total_records"),
                      "next_page_token": body.get("next_page_token", "")}
            break
    print("        sample: " + json.dumps(sample, indent=2, default=str)[:600])
    print()
    return body

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 — Light/Medium tier: users, rooms, locations

# COMMAND ----------

users_body = probe("/users", "/users", {"status": "active", "page_size": 2})

# Auto-pick a sample user id if not provided
if SAMPLE_USER_ID is None and users_body and users_body.get("users"):
    SAMPLE_USER_ID = users_body["users"][0]["id"]
print(f"Using sample_user_id = {SAMPLE_USER_ID}")

if SAMPLE_USER_ID:
    probe("/users/{id}", f"/users/{SAMPLE_USER_ID}")

probe("/groups", "/groups", {"page_size": 5})

# COMMAND ----------

rooms_body = probe("/rooms", "/rooms", {"page_size": 2})

sample_room_id = None
if rooms_body and rooms_body.get("rooms"):
    sample_room_id = rooms_body["rooms"][0]["id"]
    probe("/rooms/{id} (capacity)", f"/rooms/{sample_room_id}")
    probe("/rooms/{id}/devices", f"/rooms/{sample_room_id}/devices")

probe("/rooms/locations (country)", "/rooms/locations", {"type": "country", "page_size": 5})
probe("/rooms/locations (building)", "/rooms/locations", {"type": "building", "page_size": 5})

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 — Per-user meetings (the validated path for R2/R3)

# COMMAND ----------

sample_meeting_id = None
sample_meeting_uuid = None

if SAMPLE_USER_ID:
    for mtype in ("scheduled", "upcoming", "previous_meetings", "live"):
        body = probe(f"/users/{{id}}/meetings type={mtype}",
                     f"/users/{SAMPLE_USER_ID}/meetings",
                     {"type": mtype, "page_size": 3})
        if mtype == "previous_meetings" and body and body.get("meetings"):
            sample_meeting_id = body["meetings"][0]["id"]
            sample_meeting_uuid = body["meetings"][0]["uuid"]

print(f"sample_meeting_id={sample_meeting_id}  sample_meeting_uuid={sample_meeting_uuid}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 — Past-meeting detail & participants (numeric id preferred)

# COMMAND ----------

if sample_meeting_id:
    probe("/past_meetings/{id} (summary)", f"/past_meetings/{sample_meeting_id}")
    probe("/past_meetings/{id}/participants",
          f"/past_meetings/{sample_meeting_id}/participants",
          {"page_size": 5})

# UUID path — needs URL encoding; double-encode if starts with / or contains //
if sample_meeting_uuid:
    uuid = sample_meeting_uuid
    if uuid.startswith("/") or "//" in uuid:
        uuid_enc = urllib.parse.quote(urllib.parse.quote(uuid, safe=""), safe="")
    else:
        uuid_enc = urllib.parse.quote(uuid, safe="")
    probe("/past_meetings/{uuid}", f"/past_meetings/{uuid_enc}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.4 — Dashboard & Report (optional — may 429 on daily quota)

# COMMAND ----------

dash_body = probe("/metrics/meetings (Heavy)", "/metrics/meetings",
      {"type": "past", "from": FROM_DATE, "to": TO_DATE, "page_size": 2})

# Pick a dashboard meeting uuid to test the participants sub-endpoint.
# Fall back to the uuid we already captured from /users/{id}/meetings if
# the Dashboard listing was empty or 429-throttled.
dash_meeting_uuid = None
if dash_body and dash_body.get("meetings"):
    dash_meeting_uuid = dash_body["meetings"][0].get("uuid")
dash_meeting_uuid = dash_meeting_uuid or sample_meeting_uuid

if dash_meeting_uuid:
    uuid = dash_meeting_uuid
    if uuid.startswith("/") or "//" in uuid:
        uuid_enc = urllib.parse.quote(urllib.parse.quote(uuid, safe=""), safe="")
    else:
        uuid_enc = urllib.parse.quote(uuid, safe="")
    probe("/metrics/meetings/{uuid}/participants (Heavy)",
          f"/metrics/meetings/{uuid_enc}/participants",
          {"type": "past", "page_size": 5})
else:
    print("[skip] /metrics/meetings/{uuid}/participants — no uuid available")

if SAMPLE_USER_ID:
    probe("/report/users/{id}/meetings", f"/report/users/{SAMPLE_USER_ID}/meetings",
          {"from": FROM_DATE, "to": TO_DATE, "type": "past", "page_size": 2})

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.5 — Recordings (optional, requires `recording:read:*` scope)

# COMMAND ----------

probe("/accounts/me/recordings", "/accounts/me/recordings",
      {"from": FROM_DATE, "to": TO_DATE, "page_size": 5})

if SAMPLE_USER_ID:
    probe("/users/{id}/recordings", f"/users/{SAMPLE_USER_ID}/recordings",
          {"from": FROM_DATE, "to": TO_DATE, "page_size": 5})

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 — Summary
# MAGIC
# MAGIC Read the `[STATUS]` column above:
# MAGIC - `[200]` = endpoint healthy in this workspace.
# MAGIC - `[401]` / `[4711]` = scope missing → ask Zoom team to add (see §2).
# MAGIC - `[429]` = rate-limit exhausted → check `X-RateLimit-Type` header (`daily` waits for 00:00 GMT; `QPS` retries in seconds).
# MAGIC - `[3001]` on `/past_meetings/{id}` = the specific meeting has no past session — try a different id.
# MAGIC - `[404]` / `[300]` = endpoint not available on your plan tier.
# MAGIC
# MAGIC When all required endpoints are green, run `01_run.py` to start the real connector.
