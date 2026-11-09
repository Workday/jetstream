"""Local shim for the Jetstream platform framework.

The platform team will eventually ship a shared library that owns:
  * UUIDv7 generation
  * parameters.yaml → JSON Schema compilation
  * source_config validation
  * connector_registry / connector_manifest / connector_instance bootstrap
  * a Databricks Jobs runtime-context reader

Until that library exists, every connector repo carries its own minimal copy
under `src/jetstream/`. When the shared library ships, replace this package
with an import of it and delete these files. The public entry points are
kept small so the swap is trivial.

Public API stability contract (do not change without cross-connector review):
    ids.uuid7()
    ctx.DatabricksContext, ctx.MetaCtx, ctx.resolve_metadata_catalog, ctx.read_databricks_context
    params.compile_parameters_yaml(path) -> dict (JSON Schema)
    validate.validate_source_config(source_config, config_schema) -> None (raises on error)
    bootstrap.ensure_registered_and_get_instance(...)
"""
