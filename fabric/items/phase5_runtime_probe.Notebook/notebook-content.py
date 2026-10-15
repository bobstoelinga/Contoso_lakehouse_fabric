# Fabric notebook source

# METADATA ********************
# META {
# META   "kernel_info": {"name": "synapse_pyspark"},
# META   "dependencies": {}
# META }

# PARAMETERS CELL ********************
expected_framework_version = "0.1.0"
expected_phase5_version = "0.1.0"

# METADATA ********************
# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************
import json

import notebookutils
from pyspark.sql import functions as F

from fabric_etl import __version__ as framework_version
from fabric_etl_phase5 import __version__ as phase5_version
from fabric_etl_phase5.ingestion import RAW_IDENTITY_FIELDS, raw_record_identity

identity = {
    "project_id": "contoso",
    "source_system_id": "SALES",
    "delivery_id": "SALES|2026-09-01",
    "source_object_id": "SALES.CUSTOMERS",
    "source_file": "customers/part-000.json",
    "file_sha256": "a" * 64,
    "payload_sha256": "b" * 64,
    "duplicate_ordinal": 2,
    "version": "fabric-raw-record-v1",
}
expected_hash = raw_record_identity(**{
    name: value
    for name, value in identity.items()
    if name != "version"
})
spark_hash = spark.createDataFrame([identity]).select(
    F.sha2(
        F.to_json(F.struct(*[
            F.col(name).alias(name)
            for name in RAW_IDENTITY_FIELDS
        ])),
        256,
    ).alias("raw_record_id"),
).first()["raw_record_id"]
if framework_version != expected_framework_version:
    raise RuntimeError(f"Unexpected fabric-etl version: {framework_version}")
if phase5_version != expected_phase5_version:
    raise RuntimeError(f"Unexpected fabric-etl-phase5 version: {phase5_version}")
if spark_hash != expected_hash:
    raise RuntimeError(f"Spark/Python Raw-ID mismatch: {spark_hash} != {expected_hash}")

result = {
    "status": "SUCCEEDED",
    "scope": "PHASE5_PACKAGE_AND_SPARK_ID_VECTOR",
    "framework_version": framework_version,
    "phase5_version": phase5_version,
    "raw_record_id": spark_hash,
}
notebookutils.notebook.exit(json.dumps(result, sort_keys=True))

# METADATA ********************
# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
