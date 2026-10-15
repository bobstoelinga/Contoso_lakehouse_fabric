# Fabric notebook source

# METADATA ********************
# META {
# META   "kernel_info": {"name": "synapse_pyspark"},
# META   "dependencies": {}
# META }

# PARAMETERS CELL ********************
environment_json = "{}"
expected_phase5_version = "0.3.0"

# METADATA ********************
# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************
import json

import notebookutils
from pyspark.sql import functions as F

from fabric_etl.control import ControlStore, literal, rows
from fabric_etl.contracts import Environment
from fabric_etl_phase5 import __version__
from fabric_etl_phase5.ingestion import RAW_RECORD_COLUMNS, _output_fingerprint, spark_schema
from fabric_etl_phase5.quality import QualityBlocked, Rule, evaluate_quality, frame_fingerprint

if __version__ != expected_phase5_version:
    raise RuntimeError("Window probe package differs from its pinned version")
payload_schema = spark_schema([
    {"name": "customer_id", "data_type": "STRING", "nullable": False},
    {"name": "skip_record", "data_type": "BOOLEAN", "nullable": False},
])
payloads = [
    {"customer_id": "A", "skip_record": False},
    {"customer_id": "A", "skip_record": True},
    {"customer_id": "B", "skip_record": False},
    {"customer_id": "B", "skip_record": False},
]
values = []
for index, payload in enumerate(payloads):
    row = {name: "window-proof" for name in RAW_RECORD_COLUMNS}
    row.update(raw_record_id=f"record-{index}", delivery_sequence=1, duplicate_ordinal=index + 1,
               payload_json=json.dumps(payload), load_date_utc="2026-10-11T00:00:00+00:00")
    values.append(tuple(row[name] for name in RAW_RECORD_COLUMNS))
records = spark.createDataFrame(values, list(RAW_RECORD_COLUMNS)).cache()
try:
    expression = "count(*) OVER (PARTITION BY customer_id) = 1"
    rules = [Rule("DQ-CUST-001", expression, "ERROR", "DUPLICATE_CUSTOMER", 100, "CONTINUE")]
    outcomes, metrics = evaluate_quality(
        records, payload_schema=payload_schema, rules=rules, filter_expression="skip_record",
    )
    observed = {
        row["raw_record_id"]: (row["terminal_status"], row["error_codes"], row["warning_codes"])
        for row in outcomes.collect()
    }
    expected = {
        "record-0": ("PASSED", [], []),
        "record-1": ("FILTERED", [], []),
        "record-2": ("REJECTED", ["DUPLICATE_CUSTOMER"], []),
        "record-3": ("REJECTED", ["DUPLICATE_CUSTOMER"], []),
    }
    if observed != expected or metrics["counts"] != {"FILTERED": 1, "PASSED": 1, "REJECTED": 2}:
        raise RuntimeError("Filtered duplicate contaminated the eligible window population")
    if metrics["rules"][0]["population"] != 3 or metrics["rules"][0]["violations"] != 2:
        raise RuntimeError("Window rule metrics differ from eligible population")
    try:
        evaluate_quality(
            records, payload_schema=payload_schema,
            rules=[Rule("DQ-CUST-001", expression, "ERROR", "DUPLICATE_CUSTOMER", 66, "FAIL_BATCH")],
            filter_expression="skip_record",
        )
    except QualityBlocked as exc:
        threshold_failure = str(exc)
        if threshold_failure != "Quality thresholds blocked: DQ-CUST-001:FAIL_BATCH":
            raise
    else:
        raise RuntimeError("Window threshold failed to block 2/3 violations above 66 percent")
    empty_outcomes, empty_metrics = evaluate_quality(
        records, payload_schema=payload_schema, rules=rules, filter_expression="TRUE",
    )
    if empty_metrics["rules"][0]["population"] != 0 or empty_metrics["rules"][0]["violations"] != 0:
        raise RuntimeError("All-filtered window population is not empty")
    if empty_metrics["counts"] != {"FILTERED": 4, "PASSED": 0, "REJECTED": 0}:
        raise RuntimeError("All-filtered terminal accounting differs")
    if empty_outcomes.where((F.size("error_codes") > 0) | (F.size("warning_codes") > 0)).count():
        raise RuntimeError("Filtered records retained rule failure codes")
    result = {
        "status": "SUCCEEDED", "scope": "PHASE5_QUALITY_POST_FILTER_WINDOW",
        "phase5_version": __version__, "metrics": metrics,
        "terminal_records": observed, "all_filtered_metrics": empty_metrics,
        "threshold_failure": threshold_failure, "business_data_written": False,
        "production_release_accepted": False,
    }
finally:
    records.unpersist()
environment = Environment.from_dict(json.loads(environment_json))
scope = (
    (F.col("project_id") == environment.project_id)
    & (F.col("source_system_id") == "PHASE4_PROOF")
    & (F.col("delivery_id") == "PHASE5_QUALITY_PROBE|V1")
)
bronze = spark.table(environment.spark_table(
    "processing", "bronze", "phase5_quality_customers",
)).where(scope).select(*RAW_RECORD_COLUMNS)
if _output_fingerprint(bronze) != (
    5, "c4e6be1207f140d89bd8b4bff32c2a067a81649b36f4c656190a18d6276884ac",
):
    raise RuntimeError("Original committed scalar Bronze fixture changed")
scalar_schema = spark_schema([
    {"name": "customer_id", "data_type": "STRING", "nullable": True},
    {"name": "display_name", "data_type": "STRING", "nullable": False},
    {"name": "skip_record", "data_type": "BOOLEAN", "nullable": False},
])
scalar_rules = [
    Rule("key_present", "customer_id IS NOT NULL", "ERROR", "MISSING_KEY", 25, "FAIL_BATCH"),
    Rule("key_nonblank", "length(trim(customer_id)) > 0", "ERROR", "INVALID_KEY", 25, "FAIL_BATCH"),
    Rule("name_nonblank", "length(trim(display_name)) > 0", "WARNING", "EMPTY_NAME", 100, "CONTINUE"),
]
scalar, accounting = evaluate_quality(
    bronze, payload_schema=scalar_schema, rules=scalar_rules, filter_expression="skip_record",
)
expected_outputs = {
    "PASSED": ("quality", "phase5_probe_customers", 3,
               "0805bc92cd3f3d10d31e72bed448e3d9e4bfef848480491172c225cb6a450d7d"),
    "REJECTED": ("reject", "phase5_probe_customers", 1,
                 "37a68e86b86beeed4e680387df0111e54d450a2d9d05e58eec00fb641604310a"),
    "FILTERED": ("quality", "phase5_probe_filtered_customers", 1,
                 "c8ec6b1cb8bfecbb318bcbdccc224c9b5d7194af3d227ef580915d974258fe27"),
}
scalar_fingerprints = {}
for status, (schema, table, count, expected_hash) in expected_outputs.items():
    candidate = scalar.where(F.col("terminal_status") == status)
    target = spark.table(environment.spark_table("processing", schema, table)).where(scope)
    expected = (count, expected_hash)
    if frame_fingerprint(candidate, "raw_record_id") != expected:
        raise RuntimeError(f"New runtime changed scalar output: {status}")
    if frame_fingerprint(target, "raw_record_id") != expected:
        raise RuntimeError(f"Original scalar target changed: {status}")
    scalar_fingerprints[status] = expected_hash
control = ControlStore(spark, environment)
commits = rows(spark, f"SELECT layer, commit_id, status FROM {control.table('commit_manifest')} "
               f"WHERE batch_id = {literal('40c5a8d2-e8ee-572b-9aeb-61b1b7f3ecab')}")
expected_commits = {
    "BRONZE": "e085ca73f0d021bf58dcb3043402257d45aff233b9940efe5e87aaa2f32c6b55",
    "QUALITY": "539fae8c54e89b41275dea518f3100c85fd5f8372290df72a0a0883ed49d40a1",
}
if len(commits) != 2 or {row["layer"]: row["commit_id"] for row in commits} != expected_commits:
    raise RuntimeError("Original scalar commits changed")
if any(row["status"] != "COMMITTED" for row in commits):
    raise RuntimeError("Original scalar commits are no longer committed")
result["scalar_regression"] = {
    "accounting": accounting, "fingerprints": scalar_fingerprints,
    "original_commits": expected_commits, "targets_unchanged": True,
    "new_runtime_committed_as_old_runtime": False,
}
notebookutils.notebook.exit(json.dumps(result, sort_keys=True))
