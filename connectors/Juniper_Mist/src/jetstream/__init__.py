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
    uuid_generator.uuid7()
    databricks_context.DatabricksContext, MetaCtx, resolve_metadata_catalog, read_databricks_context
    parameter_compiler.compile_parameters_yaml(path) -> dict (JSON Schema)
    config_validator.validate_source_config(source_config, config_schema) -> None (raises on error)
    platform_bootstrap.ensure_registered_and_get_instance(...)
"""
