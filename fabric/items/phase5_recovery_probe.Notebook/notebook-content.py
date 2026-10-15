# Fabric notebook source

# METADATA ********************
# META {
# META   "kernel_info": {"name": "synapse_pyspark"},
# META   "dependencies": {}
# META }

# PARAMETERS CELL ********************
environment_json = "{}"

# METADATA ********************
# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************
import json
from dataclasses import replace
from datetime import datetime, timezone
from uuid import uuid4

import notebookutils
from pyspark.sql import functions as F

from fabric_etl.contracts import ContractError, Environment
from fabric_etl.control import ControlStore, literal, rows
from fabric_etl_phase5 import __version__
from fabric_etl_phase5.quality import commit_layer, frame_fingerprint, write_immutable_output

if __version__ != "0.2.0":
    raise RuntimeError("Recovery probe requires the exact published 0.2.0 runtime")
environment = Environment.from_dict(json.loads(environment_json))
control = ControlStore(spark, environment)
BATCH_ID = "40c5a8d2-e8ee-572b-9aeb-61b1b7f3ecab"
COMMIT_ID = "539fae8c54e89b41275dea518f3100c85fd5f8372290df72a0a0883ed49d40a1"
scope = {
    "project_id": environment.project_id, "source_system_id": "PHASE4_PROOF",
    "delivery_id": "PHASE5_QUALITY_PROBE|V1",
}
target = environment.spark_table("processing", "quality", "phase5_probe_customers")
condition = F.lit(True)
for name, value in scope.items():
    condition = condition & (F.col(name) == value)
expected_count = 3
expected_fingerprint = "0805bc92cd3f3d10d31e72bed448e3d9e4bfef848480491172c225cb6a450d7d"
owner = control.claim(f"phase5-recovery-probe:{uuid4()}", now=datetime.now(timezone.utc))
failures = {}
try:
    committed = rows(spark, f"SELECT * FROM {control.table('commit_manifest')} "
                     f"WHERE commit_id = {literal(COMMIT_ID)}")
    if len(committed) != 1 or committed[0]["status"] != "COMMITTED" or committed[0]["batch_id"] != BATCH_ID:
        raise RuntimeError("Recovery probe requires its exact previous Quality commit")
    original_commit = committed[0]
    baseline = spark.table(target).where(condition)
    count, digest = frame_fingerprint(baseline, "raw_record_id")
    if count != expected_count or digest != expected_fingerprint:
        raise RuntimeError("Recovery baseline differs from the independently recorded successful Quality job")

    def attempt(name, frame, attempted_owner, message):
        try:
            write_immutable_output(
                control, attempted_owner, frame, role="processing", schema="quality",
                table="phase5_probe_customers", key="raw_record_id", scope=scope,
            )
        except ContractError as exc:
            if message not in str(exc):
                raise RuntimeError(f"Recovery test {name} failed for an unexpected reason: {exc}") from exc
            failures[name] = str(exc)
        else:
            raise RuntimeError(f"Recovery test {name} unexpectedly allowed a write")

    attempt("payload_conflict", baseline.withColumn("typed_payload_json", F.lit('{"tampered":true}')),
            owner, "conflicts with deterministic input")
    attempt("stale_owner", baseline, replace(owner, generation=owner.generation - 1), "OWNERSHIP_LOST")
    attempt("out_of_scope", baseline.withColumn("delivery_id", F.lit("OUT_OF_SCOPE")),
            owner, "outside its declared scope")
    attempt("duplicate_key", baseline.unionByName(baseline.limit(1)), owner, "key is duplicated")
    attempt("null_key", baseline.withColumn("raw_record_id", F.lit(None).cast("string")),
            owner, "key cannot be NULL")
    try:
        commit_layer(
            control, owner, batch_id=BATCH_ID, layer="QUALITY", entity="PROBE.QUALITY_CUSTOMERS",
            input_fingerprint="conflicting-input", outputs={"conflicting": True},
            now=datetime.now(timezone.utc),
        )
    except ContractError as exc:
        if "conflicts with an existing batch/entity" not in str(exc):
            raise RuntimeError(f"Commit conflict probe failed for an unexpected reason: {exc}") from exc
        failures["commit_conflict"] = str(exc)
    else:
        raise RuntimeError("Conflicting Quality commit was unexpectedly accepted")
    count, digest = frame_fingerprint(spark.table(target).where(condition), "raw_record_id")
    if count != expected_count or digest != expected_fingerprint:
        raise RuntimeError("Negative recovery tests changed the committed Quality target")
    committed = rows(spark, f"SELECT * FROM {control.table('commit_manifest')} "
                     f"WHERE batch_id = {literal(BATCH_ID)} AND layer = 'QUALITY' "
                     "AND entity_id = 'PROBE.QUALITY_CUSTOMERS'")
    if len(committed) != 1 or committed[0] != original_commit:
        raise RuntimeError("Negative recovery tests changed or duplicated the committed manifest")
finally:
    control.release(owner)

notebookutils.notebook.exit(json.dumps({
    "status": "SUCCEEDED", "scope": "PHASE5_NEGATIVE_RECOVERY_CONTRACTS",
    "expected_failures": failures, "target_unchanged": True, "commit_unchanged": True,
    "record_count": count, "output_fingerprint": digest, "owner_released": True,
    "platform_least_privilege_proven": False,
}, sort_keys=True))
