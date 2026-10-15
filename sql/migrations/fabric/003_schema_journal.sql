CREATE TABLE IF NOT EXISTS {control}.control.schema_migration (
    project_id STRING NOT NULL, migration_id STRING NOT NULL,
    source_sha256 STRING NOT NULL, rendered_sha256 STRING NOT NULL,
    status STRING NOT NULL, applied_at TIMESTAMP NOT NULL
) USING DELTA;
