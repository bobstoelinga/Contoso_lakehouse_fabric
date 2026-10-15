"""Compatibility import for the separately installed Phase 5 extension."""

from fabric_etl_phase5.ingestion import (
    FORMAT_OPTIONS,
    RAW_IDENTITY_FIELDS,
    RAW_RECORD_COLUMNS,
    RESERVED_COLUMNS,
    raw_record_identity,
    read_manifest_records,
    record_bronze_commit,
    spark_schema,
    validate_file_inventory,
    write_bronze_delivery,
)

__all__ = [
    "FORMAT_OPTIONS",
    "RAW_IDENTITY_FIELDS",
    "RAW_RECORD_COLUMNS",
    "RESERVED_COLUMNS",
    "raw_record_identity",
    "read_manifest_records",
    "record_bronze_commit",
    "spark_schema",
    "validate_file_inventory",
    "write_bronze_delivery",
]
