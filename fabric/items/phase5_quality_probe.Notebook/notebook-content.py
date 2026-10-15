# Fabric notebook source

# METADATA ********************
# META {
# META   "kernel_info": {"name": "synapse_pyspark"},
# META   "dependencies": {}
# META }

# PARAMETERS CELL ********************
environment_json = "{}"
expected_framework_version = "0.1.0"
expected_phase5_version = "0.2.0"

# METADATA ********************
# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************
import hashlib
import json
from dataclasses import asdict
from datetime import datetime, timezone
from uuid import uuid4

import notebookutils
from pyspark.sql import functions as F

from fabric_etl import __version__ as framework_version
from fabric_etl.context import BatchContext
from fabric_etl.control import ControlStore, literal, rows
from fabric_etl.contracts import Environment, canonical_json, fingerprint
from fabric_etl.delivery import Manifest
from fabric_etl_phase5 import __version__ as phase5_version
from fabric_etl_phase5.ingestion import (
    read_manifest_records, record_bronze_commit, spark_schema,
    validate_file_inventory, write_bronze_delivery,
)
from fabric_etl_phase5.quality import (
    QualityBlocked, Rule, commit_layer, evaluate_quality, write_immutable_output,
)

SOURCE_SYSTEM_ID = "PHASE4_PROOF"
SOURCE_OBJECT_ID = "PROBE.QUALITY_CUSTOMERS"
DELIVERY_ID = "PHASE5_QUALITY_PROBE|V1"
RELEASE_ID = "phase5-quality-probe-v1"
RAW_TEXT = (
    '{"customer_id":"Q-001","display_name":"Synthetic valid","skip_record":false}\n'
    '{"customer_id":"Q-001","display_name":"Synthetic valid","skip_record":false}\n'
    '{"customer_id":"Q-002","display_name":"","skip_record":false}\n'
    '{"customer_id":null,"display_name":"Missing key","skip_record":false}\n'
    '{"customer_id":"Q-FILTERED","display_name":"Excluded fixture","skip_record":true}\n'
)
RAW_SHA256 = "bd1c9c264db11ebbbd411dd2d8ec66d0625179bc0e9412c72b6ecddaa806afed"
if len(RAW_TEXT.encode()) != 368 or hashlib.sha256(RAW_TEXT.encode()).hexdigest() != RAW_SHA256:
    raise RuntimeError("Synthetic Quality fixture differs from its pinned bytes")
if framework_version != expected_framework_version or phase5_version != expected_phase5_version:
    raise RuntimeError("Quality probe package version differs from its pinned runtime")

environment = Environment.from_dict(json.loads(environment_json))
control = ControlStore(spark, environment)
now = datetime.now(timezone.utc)
owner = control.claim(f"phase5-quality-probe:{uuid4()}", now=now)
records = None
outcomes = None
try:
    candidate = BatchContext.create(
        environment.project_id, SOURCE_SYSTEM_ID, DELIVERY_ID, RELEASE_ID, load_date=now,
    )
    existing = rows(spark, f"SELECT delivery_sequence FROM {control.table('batch_context')} "
                    f"WHERE batch_id = {literal(candidate.batch_id)}")
    if len(existing) > 1:
        raise RuntimeError("Synthetic Quality batch context is duplicated")
    if existing:
        sequence = existing[0]["delivery_sequence"]
    else:
        previous = rows(spark, f"SELECT MAX(delivery_sequence) AS sequence "
                        f"FROM {control.table('batch_context')} "
                        f"WHERE project_id = {literal(environment.project_id)} "
                        f"AND source_system_id = {literal(SOURCE_SYSTEM_ID)}")
        sequence = (previous[0]["sequence"] or 0) + 1
    batch = control.batch_context(
        owner, SOURCE_SYSTEM_ID, DELIVERY_ID, sequence, RELEASE_ID, now=now, proof_only=True,
    )
    document = {
        "contract_version": "1.0", "project_id": environment.project_id,
        "source_system_id": SOURCE_SYSTEM_ID, "delivery_id": DELIVERY_ID,
        "delivery_sequence": sequence, "schema_contract_version": RELEASE_ID,
        "status": "CLOSED", "objects": [{
            "source_object_id": SOURCE_OBJECT_ID,
            "files": [{"path": "customers.json", "size_bytes": 368, "sha256": RAW_SHA256}],
            "complete_snapshot": False, "empty_snapshot": False,
        }],
    }
    manifest = Manifest.from_dict(
        document, project_id=environment.project_id, source_system_id=SOURCE_SYSTEM_ID,
        required_objects={SOURCE_OBJECT_ID}, schema_contract_version=RELEASE_ID,
    )
    root = (f"abfss://{environment.workspace_name}@onelake.dfs.fabric.microsoft.com/"
            f"{environment.lakehouses['processing'].name}.Lakehouse/Files/phase5-proof/quality-probe-v1")
    expected_files = {
        "customers.json": RAW_TEXT, "_manifest.json": canonical_json(document).decode(),
    }
    notebookutils.fs.mkdirs(root)
    entries = notebookutils.fs.ls(root)
    if any(entry.isDir for entry in entries) or set(entry.name for entry in entries) - set(expected_files):
        raise RuntimeError("Quality Raw folder contains unexpected files or directories")
    present = {entry.name for entry in entries}
    for name, content in expected_files.items():
        path = f"{root}/{name}"
        if name not in present and not notebookutils.fs.put(path, content, False):
            raise RuntimeError("Quality Raw fixture could not be created without overwrite")
        if notebookutils.fs.head(path, max_bytes=len(content.encode()) + 1) != content:
            raise RuntimeError(f"Quality Raw fixture readback differs: {name}")
    entries = notebookutils.fs.ls(root)
    if any(entry.isDir for entry in entries) or {entry.name for entry in entries} != set(expected_files):
        raise RuntimeError("Quality Raw inventory differs after creation")
    if any(entry.size != len(expected_files[entry.name].encode()) for entry in entries):
        raise RuntimeError("Quality Raw file size differs after readback")
    validate_file_inventory(manifest, [entry.name for entry in entries])
    payload_schema = spark_schema([
        {"name": "customer_id", "data_type": "STRING", "nullable": True},
        {"name": "display_name", "data_type": "STRING", "nullable": False},
        {"name": "skip_record", "data_type": "BOOLEAN", "nullable": False},
    ])
    records = read_manifest_records(
        spark, root_path=root, manifest=manifest, project_id=environment.project_id,
        source_formats={SOURCE_OBJECT_ID: "JSON"}, schemas={SOURCE_OBJECT_ID: payload_schema},
        batch_id=batch.batch_id, release_id=RELEASE_ID, load_date_utc=batch.load_date_utc,
    ).cache()
    bronze = write_bronze_delivery(
        spark, records, target_table=environment.spark_table("processing", "bronze", "phase5_quality_customers"),
        project_id=environment.project_id, source_system_id=SOURCE_SYSTEM_ID,
        delivery_id=DELIVERY_ID, control=control, owner=owner,
    )
    stable_bronze = {key: bronze[key] for key in ("target_table", "record_count", "output_fingerprint")}
    bronze_commit = record_bronze_commit(
        control, owner, batch_id=batch.batch_id, source_system_id=SOURCE_SYSTEM_ID,
        delivery_id=DELIVERY_ID, manifest=manifest, outputs=stable_bronze, now=now,
    )
    rules = [
        Rule("key_present", "customer_id IS NOT NULL", "ERROR", "MISSING_KEY", 25, "FAIL_BATCH"),
        Rule("key_nonblank", "length(trim(customer_id)) > 0", "ERROR", "INVALID_KEY", 25, "FAIL_BATCH"),
        Rule("name_nonblank", "length(trim(display_name)) > 0", "WARNING", "EMPTY_NAME", 100, "CONTINUE"),
    ]
    stricter = [Rule("key_present", "customer_id IS NOT NULL", "ERROR", "MISSING_KEY", 24.99, "FAIL_BATCH")]
    try:
        evaluate_quality(records, payload_schema=payload_schema, rules=stricter, filter_expression="skip_record")
    except QualityBlocked as exc:
        expected_threshold_failure = str(exc)
    else:
        raise RuntimeError("Quality threshold 24.99 did not block an exact 25-percent violation rate")
    outcomes, accounting = evaluate_quality(
        records, payload_schema=payload_schema, rules=rules, filter_expression="skip_record",
    )
    outcomes = outcomes.cache()
    if accounting["bronze_count"] != 5 or accounting["counts"] != {"FILTERED": 1, "PASSED": 3, "REJECTED": 1}:
        raise RuntimeError(f"Quality accounting differs from expected 5=1+3+1: {accounting}")
    rejected = outcomes.where(F.col("terminal_status") == "REJECTED")
    if rejected.where(F.col("error_codes") != F.array(F.lit("MISSING_KEY"), F.lit("INVALID_KEY"))).count():
        raise RuntimeError("NULL key must yield one rejected record with both failure reasons")
    if outcomes.where((F.col("terminal_status") == "PASSED") & (F.size("warning_codes") == 1)).count() != 1:
        raise RuntimeError("Warning must annotate exactly one passed record without rejecting it")
    scope = {"project_id": environment.project_id, "source_system_id": SOURCE_SYSTEM_ID, "delivery_id": DELIVERY_ID}
    outputs = {}
    for status, schema, table in (
        ("PASSED", "quality", "phase5_probe_customers"),
        ("REJECTED", "reject", "phase5_probe_customers"),
        ("FILTERED", "quality", "phase5_probe_filtered_customers"),
    ):
        outputs[status] = write_immutable_output(
            control, owner, outcomes.where(F.col("terminal_status") == status),
            role="processing", schema=schema, table=table, key="raw_record_id", scope=scope,
        )
    stable_outputs = {
        status: {key: output[key] for key in ("target_table", "record_count", "output_fingerprint")}
        for status, output in outputs.items()
    }
    quality_commit = commit_layer(
        control, owner, batch_id=batch.batch_id, layer="QUALITY", entity=SOURCE_OBJECT_ID,
        input_fingerprint=fingerprint({
            "bronze_commit": bronze_commit["commit_id"], "rules": [asdict(rule) for rule in rules],
            "schema": payload_schema.json(), "filter": "skip_record", "runtime": phase5_version,
        }),
        outputs={"targets": stable_outputs, "accounting": accounting}, now=now,
    )
finally:
    if outcomes is not None:
        outcomes.unpersist()
    if records is not None:
        records.unpersist()
    control.release(owner)

result = {
    "status": "SUCCEEDED", "scope": "PHASE5_SYNTHETIC_QUALITY_REJECT",
    "framework_version": framework_version, "phase5_version": phase5_version,
    "batch_id": batch.batch_id, "delivery_id": DELIVERY_ID,
    "bronze": bronze, "accounting": accounting, "outputs": outputs,
    "bronze_commit": bronze_commit, "quality_commit": quality_commit,
    "expected_threshold_failure": expected_threshold_failure, "owner_released": True,
}
notebookutils.notebook.exit(json.dumps(result, sort_keys=True))
