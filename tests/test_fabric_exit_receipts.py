import copy
import runpy
from pathlib import Path

import pytest

from fabric_etl.contracts import ContractError, Environment, load_json

VERIFY = runpy.run_path(str(Path(__file__).resolve().parents[1] / "deploy" / "verify-phase5-proofs.py"))["verify_exit"]


def proof():
    environment = Environment.from_dict(load_json(
        Path(__file__).resolve().parents[1] / "environments" / "contoso.dev.json",
    ))
    def delivery(name):
        count = 0 if name == "empty" else 3
        keys = ["EXIT.JSON", "EXIT.CSV", "EXIT.PARQUET"] if name == "formats" else ["EXIT.JSON"]
        case = "stop_" + name if name in {"registered", "raw", "bronze", "commit", "status"} else (
            "newer_blocked" if name == "newer" else name
        )
        return {
            "batch_id": name, "load_date_utc": "2026-10-11T00:00:00Z",
            "outputs": {key: {
                "target_table": environment.spark_table(
                    "processing", "bronze", "phase5_exit_" + case + "_" + key.split(".")[1].lower(),
                ), "record_count": count, "inserted_count": count,
                "retry_count": 0, "output_fingerprint": "a" * 64,
            } for key in keys},
            "commit": {"status": "COMMITTED", "commit_id": "b" * 64},
            "audit_id": "c" * 64, "work_status": "SUCCEEDED", "delivery_status": "SUCCEEDED",
            "attempt": 1,
            "files": [] if name == "empty" else [
                {"path": key.split(".")[1].lower() + "." + key.split(".")[1].lower(),
                 "size_bytes": 3, "sha256": "d" * 64} for key in keys
            ],
        }
    value = {
        "scope": "PHASE5_EXIT_INGESTION_CONTROL", "phase5_version": "0.6.1",
        "owner_released": True, "business_data_written": False, "production_release_accepted": False,
        "cross_job_interrupted": True,
        "negative_cases": {key: "expected failure" for key in [
            "schema_JSON", "schema_CSV", "schema_PARQUET", "parquet_type",
            "registered_manifest_conflict", "physical_manifest_conflict", "incomplete_object_scope",
            "physical_file_bytes", "physical_file_missing", "missing_is_not_empty", "older_delivery_blocks_newer",
        ]},
        "interruptions": {key: key for key in ["registered", "raw", "bronze", "commit", "status"]},
        "deliveries": {key: delivery(key) for key in [
            "formats", "registered", "newer", "raw", "bronze", "commit", "status", "empty",
        ]},
        "formats": {key: {
            "count": 3, "exact_equal": True, "partitions": [1, 7],
            "identity_multiset": [["1" * 64, "A", 1], ["2" * 64, "A", 2], ["3" * 64, "B", 1]],
        } for key in ["JSON", "CSV", "PARQUET"]},
        "cross_pending": {
            "batch_id": "cross_job",
            "target_table": delivery("cross_job")["outputs"]["EXIT.JSON"]["target_table"], "record_count": 3,
            "output_fingerprint": "a" * 64, "commit_count": 0, "delivery_status": "PROCESSING",
        },
    }
    retry = copy.deepcopy(value)
    retry.update(cross_job_interrupted=False, cross_pending=None)
    del retry["negative_cases"]["older_delivery_blocks_newer"]
    retry["interruptions"] = {key: "PREVIOUS_INTERRUPTION_RECOVERED" for key in value["interruptions"]}
    for item in retry["deliveries"].values():
        item["commit"]["status"] = "VERIFIED"
        for output in item["outputs"].values():
            output["inserted_count"], output["retry_count"] = 0, output["record_count"]
    retry["deliveries"]["cross_job"] = delivery("cross_job")
    output = retry["deliveries"]["cross_job"]["outputs"]["EXIT.JSON"]
    output["inserted_count"], output["retry_count"] = 0, 3
    return value, retry


def test_exit_receipt_requires_zero_insert_retry_and_cross_job_missing_commit_repair():
    first, retry = proof()
    VERIFY(first, retry)


@pytest.mark.parametrize("change", [
    "format", "duplicate", "empty", "gate", "audit", "insert", "commit", "cross",
    "namespace", "files", "file_hash", "attempt",
])
def test_exit_receipt_rejects_success_shaped_incomplete_proofs(change):
    first, retry = proof()
    if change == "format":
        del first["formats"]["CSV"]
    elif change == "duplicate":
        first["formats"]["JSON"]["identity_multiset"][1][0] = "1" * 64
    elif change == "empty":
        first["deliveries"]["empty"]["outputs"]["EXIT.JSON"]["record_count"] = 1
    elif change == "gate":
        del first["negative_cases"]["physical_file_bytes"]
    elif change == "audit":
        first["deliveries"]["raw"]["audit_id"] = ""
    elif change == "insert":
        retry["deliveries"]["raw"]["outputs"]["EXIT.JSON"]["inserted_count"] = 1
    elif change == "commit":
        retry["deliveries"]["commit"]["commit"]["commit_id"] = "d" * 64
    elif change == "cross":
        retry["deliveries"]["cross_job"]["outputs"]["EXIT.JSON"]["output_fingerprint"] = "e" * 64
    elif change == "namespace":
        first["deliveries"]["raw"]["outputs"]["EXIT.JSON"]["target_table"] = "unbound_target"
    elif change == "files":
        first["deliveries"]["formats"]["files"].pop()
    elif change == "file_hash":
        first["deliveries"]["raw"]["files"][0]["sha256"] = ""
    else:
        first["deliveries"]["raw"]["attempt"] = 0
    with pytest.raises(ContractError):
        VERIFY(first, retry)
