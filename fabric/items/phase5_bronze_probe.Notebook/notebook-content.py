# Fabric notebook source

# METADATA ********************
# META {
# META   "kernel_info": {"name": "synapse_pyspark"},
# META   "dependencies": {}
# META }

# PARAMETERS CELL ********************
environment_json = "{}"
expected_framework_version = "0.1.0"
expected_phase5_version = "0.1.0"

# METADATA ********************
# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************
import hashlib
import json
from datetime import datetime, timezone
from uuid import uuid4

import notebookutils
from pyspark.sql import functions as F

from fabric_etl import __version__ as framework_version
from fabric_etl.context import BatchContext
from fabric_etl.control import ControlStore, literal, rows
from fabric_etl.contracts import Environment, canonical_json
from fabric_etl.delivery import Manifest
from fabric_etl_phase5 import __version__ as phase5_version
from fabric_etl_phase5.ingestion import (
    RAW_RECORD_COLUMNS,
    read_manifest_records,
    record_bronze_commit,
    spark_schema,
    validate_file_inventory,
    write_bronze_delivery,
)

SOURCE_SYSTEM_ID = "PHASE4_PROOF"
SOURCE_OBJECT_ID = "PROBE.CUSTOMERS"
DELIVERY_ID = "PHASE5_BRONZE_PROBE|V1"
RELEASE_ID = "phase5-bronze-probe-v1"
SCHEMA_CONTRACT_VERSION = "phase5-probe-v1"
RAW_PATH = "customers.json"
RAW_TEXT = (
    '{"customer_id":"PHASE5-PROBE-001","display_name":"Synthetic customer"}\n'
    '{"customer_id":"PHASE5-PROBE-001","display_name":"Synthetic customer"}\n'
)
RAW_SHA256 = "d21c87b4686069725b2147364b438bc2fdb7e9c0af43c28f475718969120eed8"
TARGET_TABLE_NAME = "phase5_probe_customers"
expected_file_bytes = RAW_TEXT.encode("utf-8")
if (
    len(expected_file_bytes) != 142
    or hashlib.sha256(expected_file_bytes).hexdigest() != RAW_SHA256
):
    raise RuntimeError("Synthetic Bronze probe fixture differs from its pinned checksum")
if framework_version != expected_framework_version:
    raise RuntimeError(f"Unexpected fabric-etl version: {framework_version}")
if phase5_version != expected_phase5_version:
    raise RuntimeError(f"Unexpected fabric-etl-phase5 version: {phase5_version}")

environment = Environment.from_dict(json.loads(environment_json))
processing = environment.lakehouses["processing"]
raw_root = (
    f"abfss://{environment.workspace_name}@onelake.dfs.fabric.microsoft.com/"
    f"{processing.name}.Lakehouse/Files/phase5-proof/bronze-probe-v1"
)
target_table = environment.spark_table("processing", "bronze", TARGET_TABLE_NAME)
raw_file_path = f"{raw_root}/{RAW_PATH}"
manifest_file_path = f"{raw_root}/_manifest.json"
now = datetime.now(timezone.utc)
control = ControlStore(spark, environment)
owner = control.claim(f"phase5-bronze-probe:{uuid4()}", now=now)

try:
    candidate_context = BatchContext.create(
        environment.project_id, SOURCE_SYSTEM_ID, DELIVERY_ID, RELEASE_ID, load_date=now,
    )
    batch_table = control.table("batch_context")
    existing_context = rows(
        spark,
        f"SELECT delivery_sequence FROM {batch_table} "
        f"WHERE batch_id = {literal(candidate_context.batch_id)}",
    )
    if existing_context:
        if len(existing_context) != 1:
            raise RuntimeError("Synthetic batch context is duplicated")
        delivery_sequence = existing_context[0]["delivery_sequence"]
    else:
        previous = rows(
            spark,
            f"SELECT MAX(delivery_sequence) AS max_sequence FROM {batch_table} "
            f"WHERE project_id = {literal(environment.project_id)} "
            f"AND source_system_id = {literal(SOURCE_SYSTEM_ID)}",
        )
        delivery_sequence = (previous[0]["max_sequence"] or 0) + 1
    batch = control.batch_context(
        owner, SOURCE_SYSTEM_ID, DELIVERY_ID, delivery_sequence, RELEASE_ID,
        now=now, proof_only=True,
    )

    manifest_document = {
        "contract_version": "1.0",
        "project_id": environment.project_id,
        "source_system_id": SOURCE_SYSTEM_ID,
        "delivery_id": DELIVERY_ID,
        "delivery_sequence": delivery_sequence,
        "schema_contract_version": SCHEMA_CONTRACT_VERSION,
        "status": "CLOSED",
        "objects": [{
            "source_object_id": SOURCE_OBJECT_ID,
            "files": [{
                "path": RAW_PATH,
                "size_bytes": len(expected_file_bytes),
                "sha256": RAW_SHA256,
            }],
            "complete_snapshot": False,
            "empty_snapshot": False,
        }],
    }
    manifest = Manifest.from_dict(
        manifest_document,
        project_id=environment.project_id,
        source_system_id=SOURCE_SYSTEM_ID,
        required_objects={SOURCE_OBJECT_ID},
        schema_contract_version=SCHEMA_CONTRACT_VERSION,
    )
    manifest_text = canonical_json(manifest_document).decode("utf-8")
    expected_files = {RAW_PATH: RAW_TEXT, "_manifest.json": manifest_text}
    notebookutils.fs.mkdirs(raw_root)
    listed = notebookutils.fs.ls(raw_root)
    if any(entry.isDir for entry in listed):
        raise RuntimeError("Synthetic Raw probe folder contains an unexpected subdirectory")
    existing_files = {entry.name: entry for entry in listed}
    unexpected_files = set(existing_files) - set(expected_files)
    if unexpected_files:
        raise RuntimeError(f"Synthetic Raw probe folder contains unexpected files: {sorted(unexpected_files)}")
    for name, content in expected_files.items():
        path = f"{raw_root}/{name}"
        if name in existing_files:
            observed = notebookutils.fs.head(path, max_bytes=len(content.encode("utf-8")) + 1)
            if observed != content:
                raise RuntimeError(f"Existing synthetic Raw file differs from the pinned fixture: {name}")
        elif not notebookutils.fs.put(path, content, False):
            raise RuntimeError(f"Could not create synthetic Raw file without overwrite: {name}")

    listed = notebookutils.fs.ls(raw_root)
    if any(entry.isDir for entry in listed):
        raise RuntimeError("Synthetic Raw probe folder contains an unexpected subdirectory")
    actual_files = {entry.name: entry for entry in listed}
    if set(actual_files) != set(expected_files):
        raise RuntimeError("Synthetic Raw file inventory differs after creation")
    if actual_files[RAW_PATH].size != len(expected_file_bytes):
        raise RuntimeError("Synthetic Raw file byte count differs from its manifest")
    for name, content in expected_files.items():
        observed = notebookutils.fs.head(
            f"{raw_root}/{name}", max_bytes=len(content.encode("utf-8")) + 1,
        )
        if observed != content:
            raise RuntimeError(f"Synthetic Raw file readback differs: {name}")
    validate_file_inventory(manifest, list(actual_files))

    schemas = {
        SOURCE_OBJECT_ID: spark_schema([
            {"name": "customer_id", "data_type": "STRING", "nullable": False},
            {"name": "display_name", "data_type": "STRING", "nullable": False},
        ]),
    }
    records = read_manifest_records(
        spark,
        root_path=raw_root,
        manifest=manifest,
        project_id=environment.project_id,
        source_formats={SOURCE_OBJECT_ID: "JSON"},
        schemas=schemas,
        batch_id=batch.batch_id,
        release_id=RELEASE_ID,
        load_date_utc=batch.load_date_utc,
    )
    if records.columns != list(RAW_RECORD_COLUMNS) or records.count() != 2:
        raise RuntimeError("Synthetic Raw reader did not produce the exact two-row Bronze contract")
    if records.select("raw_record_id").distinct().count() != 2:
        raise RuntimeError("Duplicate synthetic source rows did not retain separate Raw identities")
    duplicate_payload = (
        records.groupBy("payload_sha256")
        .agg(F.count(F.lit(1)).alias("copies"))
        .where(F.col("copies") != 2)
        .limit(1)
        .count()
    )
    if duplicate_payload:
        raise RuntimeError("Synthetic Raw duplicate multiplicity differs from the closed manifest")

    write_result = write_bronze_delivery(
        spark,
        records,
        target_table=target_table,
        project_id=environment.project_id,
        source_system_id=SOURCE_SYSTEM_ID,
        delivery_id=DELIVERY_ID,
        control=control,
        owner=owner,
    )
    if write_result["record_count"] != 2 or write_result["inserted_count"] not in {0, 2}:
        raise RuntimeError("Synthetic Bronze append/readback count is outside the expected first/retry outcomes")
    stable_outputs = {
        "target_table": write_result["target_table"],
        "record_count": write_result["record_count"],
        "output_fingerprint": write_result["output_fingerprint"],
    }
    commit = record_bronze_commit(
        control,
        owner,
        batch_id=batch.batch_id,
        source_system_id=SOURCE_SYSTEM_ID,
        delivery_id=DELIVERY_ID,
        manifest=manifest,
        outputs=stable_outputs,
        now=now,
    )
finally:
    control.release(owner)

result = {
    "status": "SUCCEEDED",
    "scope": "PHASE5_SYNTHETIC_RAW_TO_BRONZE",
    "framework_version": framework_version,
    "phase5_version": phase5_version,
    "delivery_id": DELIVERY_ID,
    "batch_id": batch.batch_id,
    "record_count": write_result["record_count"],
    "inserted_count": write_result["inserted_count"],
    "retry_count": write_result["retry_count"],
    "output_fingerprint": write_result["output_fingerprint"],
    "target_table": target_table,
    "bronze_commit_status": commit["status"],
    "owner_released": True,
}
notebookutils.notebook.exit(json.dumps(result, sort_keys=True))
