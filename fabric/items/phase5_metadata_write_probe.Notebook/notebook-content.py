# Fabric notebook source

# METADATA ********************
# META {
# META   "kernel_info": {"name": "synapse_pyspark"},
# META   "dependencies": {}
# META }

# PARAMETERS CELL ********************
environment_json = "{}"
project_json = "{}"
expected_phase5_version = "0.5.0"

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
from pyspark.sql import functions as F, types as T

from fabric_etl.context import BatchContext
from fabric_etl.control import ControlStore, literal, rows
from fabric_etl.contracts import Environment, canonical_json
from fabric_etl.delivery import Manifest
from fabric_etl.project import ProjectRelease
from fabric_etl_phase5 import __version__
from fabric_etl_phase5.ingestion import (
    read_manifest_records, record_bronze_commit, validate_file_inventory, write_bronze_delivery,
)
from fabric_etl_phase5.mapping import write_metadata_quality

if __version__ != expected_phase5_version:
    raise RuntimeError("Metadata writer differs from the immutable runtime pin")
environment = Environment.from_dict(json.loads(environment_json))
project = ProjectRelease(json.loads(project_json), (), ("UNACTIVATED_CANDIDATE",))
control = ControlStore(spark, environment)
source_id = "SALES.CUSTOMERS"
delivery_id = "PHASE5_METADATA_WRITE|CUSTOMERS_V1"
table = "phase5_metadata_customers_v1"
payload = {
    "customer_key": "MC1", "customer_name": "metadata customer one", "email": None,
    "phone": "+31123", "address_line1": "street 1", "city": "utrecht",
    "state_province": "ut", "postal_code": "1234AB", "country": "nl",
    "customer_segment": "retail", "customer_since": "2020-01-01", "is_deleted": "false",
}
payloads = [payload, dict(payload, customer_key="MC2", is_deleted="not-a-boolean")]
raw_text = "\n".join(canonical_json(value).decode() for value in payloads) + "\n"
file_sha = hashlib.sha256(raw_text.encode()).hexdigest()
source_schema = T.StructType([T.StructField(name, T.StringType(), True) for name in payload])
now = datetime.now(timezone.utc)
owner = control.claim("phase5-metadata-write:" + uuid4().hex, now=now)
records = None
try:
    candidate = BatchContext.create(
        environment.project_id, "PHASE4_PROOF", delivery_id, project.release_id, load_date=now,
    )
    existing = rows(spark, f"SELECT delivery_sequence FROM {control.table('batch_context')} "
                    f"WHERE batch_id = {literal(candidate.batch_id)}")
    if len(existing) > 1:
        raise RuntimeError("Metadata proof batch context is duplicated")
    previous = rows(spark, f"SELECT MAX(delivery_sequence) AS sequence FROM {control.table('batch_context')} "
                    f"WHERE project_id = {literal(environment.project_id)} AND source_system_id = 'PHASE4_PROOF'")
    sequence = existing[0]["delivery_sequence"] if existing else (previous[0]["sequence"] or 0) + 1
    batch = control.batch_context(
        owner, "PHASE4_PROOF", delivery_id, sequence, project.release_id, now=now, proof_only=True,
    )
    manifest_document = {
        "contract_version": "1.0", "project_id": environment.project_id,
        "source_system_id": "PHASE4_PROOF", "delivery_id": delivery_id,
        "delivery_sequence": sequence, "schema_contract_version": project.release_id,
        "status": "CLOSED", "objects": [{
            "source_object_id": source_id, "files": [{
                "path": "customers.json", "size_bytes": len(raw_text.encode()), "sha256": file_sha,
            }], "complete_snapshot": False, "empty_snapshot": False,
        }],
    }
    manifest = Manifest.from_dict(
        manifest_document, project_id=environment.project_id, source_system_id="PHASE4_PROOF",
        required_objects={source_id}, schema_contract_version=project.release_id)
    root = (f"abfss://{environment.workspace_name}@onelake.dfs.fabric.microsoft.com/"
            f"{environment.lakehouses['processing'].name}.Lakehouse/Files/phase5-proof/metadata-customers-v1")
    notebookutils.fs.mkdirs(root)
    expected = {"customers.json": raw_text, "_manifest.json": canonical_json(manifest_document).decode()}
    entries = notebookutils.fs.ls(root)
    if any(entry.isDir for entry in entries) or {entry.name for entry in entries} - set(expected):
        raise RuntimeError("Unexpected metadata proof Raw inventory")
    for name, text in expected.items():
        path = root + "/" + name
        if name not in {entry.name for entry in entries} and not notebookutils.fs.put(path, text, False):
            raise RuntimeError("Cannot create metadata proof Raw without overwrite")
        if notebookutils.fs.head(path, max_bytes=len(text.encode()) + 1) != text:
            raise RuntimeError("Metadata proof Raw bytes changed")
    entries = notebookutils.fs.ls(root)
    if any(entry.isDir for entry in entries) or {entry.name for entry in entries} != set(expected):
        raise RuntimeError("Metadata proof Raw inventory differs after readback")
    if any(entry.size != len(expected[entry.name].encode()) for entry in entries):
        raise RuntimeError("Metadata proof Raw sizes differ")
    validate_file_inventory(manifest, [entry.name for entry in entries])
    records = read_manifest_records(
        spark, root_path=root, manifest=manifest, project_id=environment.project_id,
        source_formats={source_id: "JSON"}, schemas={source_id: source_schema},
        batch_id=batch.batch_id, release_id=project.release_id, load_date_utc=batch.load_date_utc,
    ).cache()
    bronze = write_bronze_delivery(
        spark, records, target_table=environment.spark_table("processing", "bronze", table),
        project_id=environment.project_id, source_system_id="PHASE4_PROOF", delivery_id=delivery_id,
        control=control, owner=owner,
    )
    bronze_commit = record_bronze_commit(
        control, owner, batch_id=batch.batch_id, source_system_id="PHASE4_PROOF", delivery_id=delivery_id,
        manifest=manifest, outputs={key: bronze[key] for key in (
            "target_table", "record_count", "output_fingerprint",
        )}, now=now,
    )
    result = write_metadata_quality(
        control, owner, records, source_schema=source_schema, project=project, source_id=source_id,
        bronze_commit_id=bronze_commit["commit_id"], table=table, now=now, proof_only=True,
    )
    if result["accounting"]["counts"] != {"PASSED": 1, "REJECTED": 1, "FILTERED": 0}:
        raise RuntimeError("Durable metadata fixture accounting differs")
    rejected = spark.table(result["outputs"]["REJECTED"]["target_table"]).where(
        F.col("batch_id") == batch.batch_id,
    ).select("error_codes").collect()
    if len(rejected) != 1 or not any(code.startswith("INVALID_CAST_") for code in rejected[0]["error_codes"]):
        raise RuntimeError("Durable rejected record is missing its explicit cast diagnostic")
    reject_error_codes = rejected[0]["error_codes"]
finally:
    if records is not None:
        records.unpersist()
    control.release(owner)
notebookutils.notebook.exit(json.dumps({
    "status": "SUCCEEDED", "scope": "PHASE5_METADATA_DURABLE_CUSTOMERS",
    "phase5_version": __version__, "release_id": project.release_id,
    "source_id": source_id, "batch_id": batch.batch_id, "load_date_utc": batch.load_date_utc,
    "bronze": bronze, "bronze_commit": bronze_commit, **result, "owner_released": True,
    "reject_error_codes": reject_error_codes,
    "production_release_accepted": False, "business_data_written": False,
}, sort_keys=True))
