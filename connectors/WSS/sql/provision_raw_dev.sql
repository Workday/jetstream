-- Example destination for a dev WSS job.
-- Replace raw_dev if destination_catalog is different.
-- Replace wss if destination_schema is different.
--
-- Tables: raw_dev.wss.<object>
-- API responses: /Volumes/raw_dev/wss/api_responses/<object>/<timestamp>/response.json
-- Attachments:   /Volumes/raw_dev/wss/attachments/
--
-- The connector creates Iceberg tables on first write. It does not create
-- the schema or the volumes.

CREATE SCHEMA IF NOT EXISTS raw_dev.wss
  COMMENT 'WSS raw tables';

CREATE VOLUME IF NOT EXISTS raw_dev.wss.attachments
  COMMENT 'WSS attachment binaries';

CREATE VOLUME IF NOT EXISTS raw_dev.wss.api_responses
  COMMENT 'WSS API response backups';

-- Example grants. Replace the principal with the job run-as identity.
-- GRANT USE CATALOG ON CATALOG raw_dev TO `<job_principal>`;
-- GRANT USE SCHEMA ON SCHEMA raw_dev.wss TO `<job_principal>`;
-- GRANT CREATE TABLE ON SCHEMA raw_dev.wss TO `<job_principal>`;
-- GRANT READ VOLUME, WRITE VOLUME ON VOLUME raw_dev.wss.attachments TO `<job_principal>`;
-- GRANT READ VOLUME, WRITE VOLUME ON VOLUME raw_dev.wss.api_responses TO `<job_principal>`;
