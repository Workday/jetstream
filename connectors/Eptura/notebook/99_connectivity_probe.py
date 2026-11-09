# Databricks notebook source
# MAGIC %md
# MAGIC # jetstream-eptura-connector — API probe
# MAGIC
# MAGIC Read-only. Writes nothing to the control plane or the destination, so it
# MAGIC is safe against any environment. Two things it answers:
# MAGIC
# MAGIC 1. **Can we authenticate?** Runs the two-step OAuth flow and fetches one
# MAGIC    record from each entity the connector syncs.
# MAGIC 2. **What else can this tenant read?** Sweeps a list of documented
# MAGIC    iOffice collections and reports which ones respond.
# MAGIC
# MAGIC The sweep exists because Eptura's OAuth response carries no scope or
# MAGIC entitlement information — the token says who you are, not what you may
# MAGIC read. Access depends on which iOffice modules the tenant licenses and on
# MAGIC the API account's role, and the only way to establish that is to call the
# MAGIC endpoint and look at the status code.

# COMMAND ----------

# MAGIC %pip install -q -r ../requirements.txt

# COMMAND ----------

import os
import sys

# Databricks runs a notebook with its own folder as the working directory, so
# the repo root is one level up. Resolved explicitly because notebooks have no
# __file__.
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..")))

# COMMAND ----------

dbutils.widgets.text("secret_scope", "", "secret_scope")
dbutils.widgets.text("api_base_path", "/external/api/rest/v2", "api_base_path")
dbutils.widgets.text("redirect_uri", "", "redirect_uri")
dbutils.widgets.text("oauth_scope", "email", "oauth_scope")
dbutils.widgets.text("extra_endpoints", "", "extra_endpoints (CSV, added to the sweep)")
dbutils.widgets.dropdown("check_incremental", "true", ["true", "false"],
                         "check_incremental (test modifiedOrCreatedAfter)")

# COMMAND ----------

import time

from src import connector_config as config_mod
from src import api_objects as objects_mod
from src.api_auth import EpturaTokens
from src.api_extractor import unwrap_payload
from src.http_client import HttpClient, HttpError

widgets = {k: dbutils.widgets.get(k) for k in
           ["secret_scope", "api_base_path", "redirect_uri", "oauth_scope"]}
# Keep the sweep responsive: an unreachable endpoint should report quickly
# rather than burn the full production retry budget.
widgets["retry_max_attempts"] = "2"

source = config_mod.resolve_run_config(widgets, dbutils=dbutils).source
http = HttpClient(source, EpturaTokens(source))

print(f"site:     {source.site_address}")
print(f"base url: {source.base_url}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. The entities this connector syncs

# COMMAND ----------

SYNCED = [o.name for o in objects_mod.ENTITIES]


def probe(path, params=None):
    """Return (status, records, total, detail).

    `status` is 'ok', an HTTP status code, or an exception class name. `detail`
    carries the failure message, which is usually the only thing that explains
    a rejected credential or a malformed request.
    """
    url = f"{source.base_url}/{path}"
    query = {"limit": 1, "startAt": 0}
    query.update(params or {})
    try:
        records, total = unwrap_payload(http.get_json(url, params=query))
        return "ok", records, total, ""
    except HttpError as exc:
        return exc.status, [], None, str(exc)
    except Exception as exc:
        return type(exc).__name__, [], None, str(exc)


for name in SYNCED:
    status, records, total, detail = probe(name)
    if status == "ok":
        fields = sorted(records[0].keys()) if records else []
        print(f"  OK   {name:<16} total={total if total is not None else '?'}")
        print(f"       fields: {fields}")
    else:
        print(f"  FAIL {name:<16} {status}  {detail}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. What else can this tenant read?
# MAGIC
# MAGIC Candidates come from the published v2 reference
# MAGIC (https://ioffice.github.io/api/). Presence here is not a promise the
# MAGIC endpoint exists on your tenant — that is exactly what the sweep
# MAGIC determines. Reading the result:
# MAGIC
# MAGIC | Result | Meaning |
# MAGIC |---|---|
# MAGIC | `OK` | Accessible; `total` is the collection size |
# MAGIC | `EMPTY` | Accessible but holds no records |
# MAGIC | `401` / `403` | Authenticated but not entitled — module or role |
# MAGIC | `404` | Not present on this tenant or API version |
# MAGIC
# MAGIC `neighborhoods` is worth noting: the connector syncs it and it returns
# MAGIC data, but it appears nowhere in the public reference.

# COMMAND ----------

# Grouped only so the output is readable; the sweep treats them identically.
CANDIDATES = {
    "core": ["rooms", "buildings", "floors", "users", "neighborhoods"],
    "lookups": ["rooms/types", "users/types", "countries", "contacts",
                "fields", "categories", "categories/items"],
    "reservations": ["reservations"],
    "assets": ["assets", "assets/types", "assets/models"],
    "visitors": ["visitors", "visitors/types"],
    "maintenance": ["maintenance/requests", "maintenance/types",
                    "maintenance/categories", "maintenance/priorities",
                    "maintenance/products", "maintenance/scheduled"],
    "move": ["move/requests", "move/requests/items", "move/statuses",
             "move/requestTypes", "move/plans", "move/tasks"],
    "mail": ["mail/items", "mail/types", "mail/routes", "mail/locations",
             "mail/mailstops", "mail/packageTypes"],
    "other": ["agreements", "approvals", "savedsearches"],
}

extra = [e.strip() for e in dbutils.widgets.get("extra_endpoints").split(",") if e.strip()]
if extra:
    CANDIDATES["extra"] = extra

check_incremental = dbutils.widgets.get("check_incremental") == "true"

# modifiedOrCreatedAfter takes epoch SECONDS, while the dateCreated /
# dateUpdated fields inside records are epoch MILLISECONDS. Asking for the
# last day and comparing totals is the only way to tell a supported filter
# from one the server silently ignores.
since_epoch_seconds = int(time.time()) - 86400

# COMMAND ----------

header = f"{'endpoint':<28} {'result':<10} {'total':>8}"
if check_incremental:
    header += f" {'since 24h':>10}  incremental"
print(header)
print("-" * len(header))

accessible, denied, incremental_ok = [], [], []

for group, paths in CANDIDATES.items():
    print(f"\n[{group}]")
    for path in paths:
        status, records, total, detail = probe(path)

        if status != "ok":
            denied.append((path, status, detail))
            print(f"{path:<28} {str(status):<10}")
            continue

        accessible.append(path)
        result = "OK" if records else "EMPTY"
        total_str = str(total) if total is not None else "?"
        line = f"{path:<28} {result:<10} {total_str:>8}"

        if check_incremental:
            if total is None:
                line += f" {'n/a':>10}  unknown (no total in response)"
            elif total == 0:
                line += f" {'n/a':>10}  untestable (collection is empty)"
            else:
                _, _, filtered, _ = probe(
                    path, {"modifiedOrCreatedAfter": since_epoch_seconds}
                )
                if filtered is None:
                    line += f" {'n/a':>10}  unknown"
                elif filtered < total:
                    incremental_ok.append(path)
                    line += f" {filtered:>10}  YES"
                else:
                    # Same count back means the parameter was ignored, or
                    # everything genuinely changed in the last day.
                    line += f" {filtered:>10}  no (filter ignored?)"

        print(line)

# COMMAND ----------

print("=" * 70)
print(f"accessible:   {len(accessible)}")
print(f"inaccessible: {len(denied)}")
if check_incremental:
    print(f"honours modifiedOrCreatedAfter: {sorted(incremental_ok) or '(none detected)'}")
print()
print("Not currently synced but readable:")
print(f"  {sorted(set(accessible) - set(SYNCED)) or '(none)'}")
print()
print("Adding one of these is a code change, not a parameter change: append an")
print("ObjectCfg to ENTITIES in src/api_objects.py. Nested paths such as")
print("'maintenance/requests' also need a path field on ObjectCfg, since")
print("src/api_extractor.py currently derives the URL from the entity name alone.")
print("=" * 70)

# COMMAND ----------

# A 401/403/404 is a real answer about entitlement, so the sweep reporting
# those is a success. Reaching nothing at all is not an answer — it means the
# cluster could not talk to Eptura, and a probe that stays green in that case
# would be worse than no probe.
if not accessible:
    # Distinct messages only: one root cause repeated 39 times is the norm,
    # and the message is what distinguishes a rejected credential from an
    # unreachable host.
    reasons = sorted({d for _, _, d in denied if d})
    raise RuntimeError(
        f"probe reached none of {len(denied)} endpoints. Distinct failures:\n  "
        + "\n  ".join(reasons)
    )
