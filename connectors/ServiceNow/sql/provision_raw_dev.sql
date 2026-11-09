-- Example destination for a dev ServiceNow job.
-- Replace raw_dev if destination_catalog is different.
-- Replace servicenow if destination_schema is different.
--
-- Tables: raw_dev.servicenow.<table>
-- Files:  /Volumes/raw_dev/servicenow/attachments/<table>/<parent_sys_id>/<attachment_sys_id>/<file_name>
--
-- The connector creates Iceberg tables on first write. It does not create
-- the schema or the volume.

CREATE SCHEMA IF NOT EXISTS raw_dev.servicenow
  COMMENT 'ServiceNow raw tables';

CREATE VOLUME IF NOT EXISTS raw_dev.servicenow.attachments
  COMMENT 'ServiceNow attachment binaries';

-- Example grants. Replace the principal with the job run-as identity.
-- GRANT USE CATALOG ON CATALOG raw_dev TO `<job_principal>`;
-- GRANT USE SCHEMA ON SCHEMA raw_dev.servicenow TO `<job_principal>`;
-- GRANT CREATE TABLE ON SCHEMA raw_dev.servicenow TO `<job_principal>`;
-- GRANT READ VOLUME, WRITE VOLUME ON VOLUME raw_dev.servicenow.attachments TO `<job_principal>`;
