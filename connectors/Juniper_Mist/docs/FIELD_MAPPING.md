# Juniper Mist → Destination Field Mapping

This document covers the API-to-destination column mappings for all 25
objects. All tables also include the three mandatory `_jetstream_*` columns.

## Mandatory metadata columns (all tables)

| Column | Type | Notes |
|---|---|---|
| `_jetstream_synced` | TIMESTAMP | Set on every MERGE operation |
| `_jetstream_deleted` | BOOLEAN | `true` = tombstoned (full-load objects only) |
| `_jetstream_source_object` | STRING | API object name (e.g. `zone_visits`) |

---

## Primary objects

### `org`
**Endpoint**: `GET /api/v1/orgs/{org_id}`
**Primary key**: `id`
**Load strategy**: full

All top-level fields returned as-is. Nested sub-dicts serialised to JSON strings.

---

### `sitegroups`
**Endpoint**: `GET /api/v1/orgs/{org_id}/sitegroups`
**Primary key**: `id`
**Load strategy**: full

| API field | Destination column | Notes |
|---|---|---|
| `id` | `id` | PK |
| `name` | `name` | |
| `site_ids` | `site_ids` | JSON string (array) |
| `org_id` | `org_id` | |

---

### `inventory`
**Endpoint**: `GET /api/v1/orgs/{org_id}/inventory`
**Primary key**: `id`
**Load strategy**: full

| API field | Destination column | Notes |
|---|---|---|
| `id` | `id` | PK — MAC-derived UUID |
| `mac` | `mac` | |
| `model` | `model` | |
| `type` | `type` | `ap` / `switch` / `gateway` |
| `serial` | `serial` | |
| `org_id` | `org_id` | |
| `site_id` | `site_id` | null if unclaimed |
| `hostname` | `hostname` | |
| `sku` | `sku` | |

---

### `device_stats`
**Endpoint**: `GET /api/v1/orgs/{org_id}/stats/devices`
**Primary key**: `id`
**Load strategy**: full

| API field | Destination column | Notes |
|---|---|---|
| `_id` | `id` | **Remapped** (`_id` → `id`) |
| `mac` | `mac` | |
| `model` | `model` | |
| `version` | `version` | |
| `status` | `status` | |
| `uptime` | `uptime` | |
| `site_id` | `site_id` | |
| `env_stat` | `env_stat` | JSON string (nested dict) |
| `radio_stat` | `radio_stat` | JSON string (nested dict) |
| `port_stat` | `port_stat` | JSON string (nested dict) |

---

### `org_wlans`
**Endpoint**: `GET /api/v1/orgs/{org_id}/wlans`
**Primary key**: `id`
**Load strategy**: full

| API field | Destination column | Notes |
|---|---|---|
| `id` | `id` | PK |
| `ssid` | `ssid` | |
| `band` | `band` | |
| `enabled` | `enabled` | |
| `hide_ssid` | `hide_ssid` | |
| `security` | `security` | JSON string |
| `vlan_enabled` | `vlan_enabled` | |
| `vlan_id` | `vlan_id` | |

---

### `sites`
**Endpoint**: `GET /api/v1/orgs/{org_id}/sites`
**Primary key**: `id`
**Load strategy**: full

| API field | Destination column | Notes |
|---|---|---|
| `id` | `id` | PK |
| `name` | `name` | |
| `org_id` | `org_id` | |
| `timezone` | `timezone` | |
| `country_code` | `country_code` | |
| `address` | `address` | |
| `latlng` | `latlng` | JSON string (dict with `lat`, `lng`) |
| `sitegroup_ids` | `sitegroup_ids` | JSON string (array) |

---

### `maps`
**Endpoint**: `GET /api/v1/sites/{site_id}/maps`
**Primary key**: `id`
**Load strategy**: full
**Parent**: `sites`

| API field | Destination column | Notes |
|---|---|---|
| `id` | `id` | PK |
| `site_id` | `site_id` | Stamped by connector |
| `name` | `name` | |
| `type` | `type` | `image` / `google_maps` / etc. |
| `width` | `width` | Pixels |
| `height` | `height` | Pixels |
| `ppm` | `ppm` | Pixels per metre |
| `flags` | `flags` | JSON string |

---

### `zones`
**Endpoint**: `GET /api/v1/sites/{site_id}/zones`
**Primary key**: `id`
**Load strategy**: full
**Parent**: `sites`

| API field | Destination column | Notes |
|---|---|---|
| `id` | `id` | PK |
| `site_id` | `site_id` | Stamped by connector |
| `name` | `name` | |
| `map_id` | `map_id` | |
| `vertices` | `vertices` | JSON string (polygon array) |

---

### `zone_stats`
**Endpoint**: `GET /api/v1/sites/{site_id}/stats/zones`
**Primary key**: `id`
**Load strategy**: full (live snapshot)
**Parent**: `sites`

| API field | Destination column | Notes |
|---|---|---|
| `id` | `id` | PK (zone id) |
| `site_id` | `site_id` | Stamped by connector |
| `name` | `name` | |
| `num_clients` | `num_clients` | Current occupancy |
| `map_id` | `map_id` | |
| `assets` | `assets` | JSON string |
| `sdkclients` | `sdkclients` | JSON string |
| `unconnected_clients` | `unconnected_clients` | JSON string |

---

### `site_wlans`
**Endpoint**: `GET /api/v1/sites/{site_id}/wlans`
**Primary key**: `id`
**Load strategy**: full
**Parent**: `sites`

Same shape as `org_wlans` with the addition of `site_id` (stamped).

---

### `site_settings`
**Endpoint**: `GET /api/v1/sites/{site_id}/setting`
**Primary key**: `site_id`
**Load strategy**: full (single record per site)
**Parent**: `sites`

The settings object is large and deeply nested. All nested sub-dicts
(`wifi`, `rtsa`, `analytic`, `rogue`, `occupancy`, `vars`, etc.) are
serialised to JSON strings — no object-specific flattening.

| Column | Notes |
|---|---|
| `site_id` | PK — stamped by connector |
| `wifi` | JSON string |
| `rtsa` | JSON string |
| `analytic` | JSON string |
| `rogue` | JSON string |
| `occupancy` | JSON string |
| `wids` | JSON string |
| ... | All other top-level fields preserved as-is or as JSON strings |

---

### `zone_visits`
**Endpoint**: `GET /api/v1/sites/{site_id}/zones/visits/search?scope=site`
**Primary key**: `(site_id, mac, enter_epoch)`
**Load strategy**: incremental\_cursor
**Parent**: `sites_filtered`
**Cursor field**: `timestamp`

| API field | Destination column | Notes |
|---|---|---|
| `site_id` | `site_id` | PK part |
| `mac` | `mac` | PK part |
| — | `enter_epoch` | **Derived**: `str(int(enter * 1000))` — millisecond epoch string, PK part |
| `timestamp` | `timestamp` | Unix epoch float — basis for incremental cursor |
| `enter` | `enter` | Enter timestamp float |
| `duration` | `duration` | Seconds in zone |
| `user_type` | `user_type` | `client` / `sdkclient` / `asset` — stamped per loop iteration |
| `scope_id` | `scope_id` | Zone id |
| `org_id` | `org_id` | |
| `random_mac` | `random_mac` | |

---

### `zone_visits_by_zone`
**Endpoint**: `GET /api/v1/sites/{site_id}/zones/visits/search?scope=zone`
**Primary key**: `(site_id, scope_id, mac, enter_epoch)`
**Load strategy**: incremental\_cursor
**Parent**: `sites_filtered`

Same as `zone_visits` with the addition of:

| Column | Notes |
|---|---|
| `zone_id` | **Derived**: alias for `scope_id` |

---

### `clients_search`
**Endpoint**: `GET /api/v1/sites/{site_id}/clients/search`
**Primary key**: `(site_id, mac, timestamp)`
**Load strategy**: incremental\_cursor
**Parent**: `sites_filtered`

| API field | Destination column | Notes |
|---|---|---|
| `site_id` | `site_id` | PK part (stamped by connector) |
| `mac` | `mac` | PK part |
| `timestamp` | `timestamp` | PK part |
| `ssid` | `ssid` | |
| `ip` | `ip` | |
| `hostname` | `hostname` | |
| `os` | `os` | |
| `os_version` | `os_version` | |
| `family` | `family` | |
| `manufacture` | `manufacture` | |
| `rssi` | `rssi` | |
| `channel` | `channel` | |
| `band` | `band` | |
| `assoc_time` | `assoc_time` | Association epoch |

---

## Optional objects

### `clients`
**Endpoint**: `GET /api/v1/sites/{site_id}/stats/clients`
**Primary key**: `(site_id, mac)` — live snapshot
**Load strategy**: full
**Parent**: `sites_filtered`

### `client_counts`
**Endpoint**: `GET /api/v1/sites/{site_id}/clients/count`
**Primary key**: `(site_id, distinct, key)`
**Load strategy**: full (count per facet)
**Parent**: `sites_filtered`

Loops over `distinct` = `device`, `ssid`, `os`. Each result row:

| Column | Notes |
|---|---|
| `site_id` | Stamped |
| `distinct` | Facet name (`device` / `ssid` / `os`) |
| `key` | Facet value |
| `count` | Number of clients matching this facet value |

### `map_clients`
**Endpoint**: `GET /api/v1/sites/{site_id}/stats/maps/{map_id}/clients`
**Primary key**: `(site_id, map_id, mac)`
**Load strategy**: full (double fan-out: sites → maps → clients)
**Parent**: `maps` (double fan-out via `{site_id, map_id}` dict cache)

### `unconnected_clients`
**Endpoint**: `GET /api/v1/sites/{site_id}/stats/maps/{map_id}/unconnected_clients`
**Primary key**: `(site_id, map_id, mac)`
**Load strategy**: full (double fan-out)
**Parent**: `maps`

### `mapstacks`, `rssizones`, `rssizone_stats`
**Load strategy**: full (per-site fan-out, all sites)
**Primary key**: `id`

### `assets`, `sdkclients`
**Load strategy**: full (per-site fan-out, filtered sites)
**Primary key**: `(site_id, mac)` / `(site_id, id)`

### `guests`
**Endpoint**: `GET /api/v1/sites/{site_id}/guests`
**Load strategy**: full (all sites)

### `guests_search`
**Endpoint**: `GET /api/v1/sites/{site_id}/guests/search`
**Load strategy**: incremental\_cursor (filtered sites)
**Primary key**: `(site_id, mac, timestamp)`
