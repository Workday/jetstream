# Field mapping

Source records land as Iceberg columns with the same names the API returns.
There is no rename layer. Nested objects and arrays stay nested
(`STRUCT` / `ARRAY`), inferred from JSON lines in `destination_writer`.

Primary key for every table: `id` (string).

Platform columns (appended, never taken from the API):

| Column | Type | Meaning |
|---|---|---|
| `_jetstream_synced` | TIMESTAMP | Last MERGE of this PK |
| `_jetstream_deleted` | BOOLEAN | True when this PK was absent from a full snapshot |

---

## rooms

Typical top-level fields: `id`, `name`, `description`, `longDescription`,
`area`, `capacity`, `occupied`, `reservable`, `reservableByRules`, `type`,
`floor`, `googleData`, `remoteInfo`, `dateCreated`, `dateUpdated`.

`type` is a struct (`type.name` is Workstation, Phone Room, Huddle Room, …).
`floor` embeds building and address. Downstream path:

`floor.building.address.state.name`

`dateCreated` / `dateUpdated` are epoch **milliseconds**.

---

## buildings

Typical fields: `id`, `name`, `code`, `address`, `location`, `metric`,
`costCenters` (array of structs), `revitLink`, `dateCreated`, `dateUpdated`.

Region-style filters use the nested array, e.g.
`filter(costCenters, x -> x.category.name = 'Region')`.

---

## floors

Typical fields: `id`, `name`, `area`, `interiorGross`, `leaseArea`,
`drawingAvailable`, `building` (struct), `dateCreated`, `dateUpdated`.

Room rows already embed floor/building; this table adds lease and drawing
attributes not present on every room payload.

---

## neighborhoods

Typical list fields: `id`, `name`, `color`, `fillOpacity`, `borderColor`,
`borderOpacity`, `borderWeight`, `center`, `dateCreated`, `dateUpdated`.

The collection payload **omits** `floor`. The connector re-fetches each row
as `GET /neighborhoods/{id}`, which returns a nested `floor` object. Join to
rooms (and to the `floors` table) on:

`neighborhoods.floor.id` = `rooms.floor.id` = `floors.id`

Not listed in the public v2 index; the tenant still returns data.

---

## users

Typical fields: `id`, `name`, `firstName`, `lastName`, `email`, `userName`,
`accountType`, `userType`, `workMode`, `isPlatformUser`, `floorWarden`,
`specialNeeds`, `color`, `dateCreated`, `dateUpdated`.

Do not treat `passwordResetAttemptsRemaining` as a business key.

---

## Adding a field

No mapping file to edit. If Eptura adds a top-level property, the next MERGE
adds the column (`ALTER TABLE` + schema-change event). Nested fields inside
an existing struct rely on `spark.databricks.delta.schema.autoMerge.enabled`
when the cluster allows it.
