import copy
import json
import runpy
from pathlib import Path

import pytest

from fabric_etl.contracts import ContractError, Environment, load_json


VERIFY = runpy.run_path(str(Path(__file__).resolve().parents[1] / "deploy" / "verify-phase5-proofs.py"))


def metadata_receipt():
    return json.loads((Path(__file__).resolve().parents[1] / "deploy" / "evidence"
                       / "phase5-metadata-quality-v1-receipt.json").read_text(encoding="utf-8"))


def test_actual_live_metadata_receipt_covers_all_rules_and_separate_job_equivalence():
    value = VERIFY["result"](metadata_receipt())
    retry = json.loads((Path(__file__).resolve().parents[1] / "deploy" / "evidence"
                       / "phase5-metadata-quality-v1-retry-receipt.json").read_text(encoding="utf-8"))
    assert retry["job"]["id"] != metadata_receipt()["job"]["id"]
    VERIFY["verify_metadata_quality"](value, VERIFY["result"](retry))


@pytest.mark.parametrize("change", ["inventory", "rule", "population", "cast", "duplicate", "release", "fingerprint", "writes"])
def test_metadata_proof_rejects_incomplete_or_success_shaped_claims(change):
    value = metadata_receipt()["job"]["exit_result"]
    if change == "inventory":
        del value["sources"]["SALES.ORDERS"]
    elif change == "rule":
        value["sources"]["SALES.ORDERS"]["accounting"]["metadata_rule_ids"].pop()
    elif change == "population":
        value["ecb_filter_accounting"]["rules"][0]["population"] = 2
    elif change == "cast":
        value["invalid_cast_accounting"]["counts"] = {"PASSED": 1, "REJECTED": 0, "FILTERED": 0}
    elif change == "duplicate":
        value["duplicate_threshold_blocked"] = False
    elif change == "release":
        value["release_id"] = "0" * 64
    elif change == "fingerprint":
        value["sources"]["SALES.CUSTOMERS"]["fingerprint"][1] = "bad"
    else:
        value["business_data_written"] = True
    with pytest.raises(ContractError):
        VERIFY["verify_metadata_quality"](value)


def test_metadata_separate_retry_must_preserve_all_output_fingerprints():
    value = metadata_receipt()["job"]["exit_result"]
    repeated = copy.deepcopy(value)
    repeated["sources"]["SALES.CUSTOMERS"]["fingerprint"][1] = "0" * 64
    with pytest.raises(ContractError, match="separate jobs"):
        VERIFY["verify_metadata_quality"](value, repeated)


def metadata_write_result():
    root = Path(__file__).resolve().parents[1]
    environment = Environment.from_dict(load_json(root / "environments" / "contoso.dev.json"))
    value = metadata_receipt()["job"]["exit_result"]
    accounting = copy.deepcopy(value["sources"]["SALES.CUSTOMERS"]["accounting"])
    accounting["bronze_count"] = 2
    accounting["counts"] = {"PASSED": 1, "REJECTED": 1, "FILTERED": 0}
    for metric in accounting["rules"]:
        metric["population"] = 2
        if metric["rule_id"] == "CAST-M-QA-CUST-12":
            metric["violations"], metric["violation_percent"] = 1, 50.0

    def output(schema, table, count):
        return {
            "target_table": environment.spark_table("processing", schema, table),
            "record_count": count, "inserted_count": count, "retry_count": 0,
            "output_fingerprint": "a" * 64,
        }

    return {
        "status": "SUCCEEDED", "scope": "PHASE5_METADATA_DURABLE_CUSTOMERS",
        "phase5_version": "0.5.0", "release_id": value["release_id"], "source_id": "SALES.CUSTOMERS",
        "batch_id": "batch", "load_date_utc": "2026-10-11T00:00:00.000000+00:00",
        "bronze": output("bronze", "phase5_metadata_customers_v1", 2),
        "outputs": {
            "PASSED": output("quality", "phase5_metadata_customers_v1", 1),
            "REJECTED": output("reject", "phase5_metadata_customers_v1", 1),
            "FILTERED": output("quality", "phase5_metadata_customers_v1_filtered", 0),
        },
        "bronze_commit": {"commit_id": "b" * 64, "status": "COMMITTED"},
        "quality_commit": {"commit_id": "c" * 64, "status": "COMMITTED"},
        "accounting": accounting, "reject_error_codes": ["INVALID_CAST_11"], "owner_released": True,
        "production_release_accepted": False, "business_data_written": False,
    }


def test_durable_metadata_verifier_accepts_exact_first_and_zero_insert_retry():
    value = metadata_write_result()
    retry = copy.deepcopy(value)
    for output in [retry["bronze"], *retry["outputs"].values()]:
        output["inserted_count"], output["retry_count"] = 0, output["record_count"]
    retry["bronze_commit"]["status"] = retry["quality_commit"]["status"] = "VERIFIED"
    VERIFY["verify_metadata_write"](value, retry)
    retry["quality_commit"]["commit_id"] = "d" * 64
    with pytest.raises(ContractError, match="retry"):
        VERIFY["verify_metadata_write"](value, retry)


def test_actual_durable_metadata_first_and_separate_retry_receipts_are_verified():
    evidence = Path(__file__).resolve().parents[1] / "deploy" / "evidence"
    receipt = json.loads((evidence
                      / "phase5-metadata-write-v1-receipt.json").read_text(encoding="utf-8"))
    retry = json.loads((evidence
                        / "phase5-metadata-write-v1-retry-receipt.json").read_text(encoding="utf-8"))
    assert receipt["job"]["id"] != retry["job"]["id"]
    VERIFY["verify_metadata_write"](VERIFY["result"](receipt), VERIFY["result"](retry))


@pytest.mark.parametrize("change", ["counts", "target", "diagnostic", "commit", "population", "owner"])
def test_durable_metadata_verifier_rejects_success_shaped_claims(change):
    value = metadata_write_result()
    if change == "counts":
        value["outputs"]["PASSED"]["record_count"] = 2
    elif change == "target":
        value["outputs"]["REJECTED"]["target_table"] = "other.table"
    elif change == "diagnostic":
        value["reject_error_codes"] = []
    elif change == "commit":
        value["quality_commit"]["status"] = "PENDING"
    elif change == "population":
        value["accounting"]["rules"][0]["population"] = 1
    else:
        value["owner_released"] = False
    with pytest.raises(ContractError):
        VERIFY["verify_metadata_write"](value)


def window_result():
    def metrics(population, violations, counts):
        return {
            "bronze_count": 4, "counts": counts,
            "rules": [{
                "rule_id": "DQ-CUST-001", "population": population, "violations": violations,
                "violation_percent": 100.0 * violations / population if population else 0.0,
                "threshold_percent": 100, "breached": False, "action": "CONTINUE",
            }],
        }
    return {
        "status": "SUCCEEDED", "scope": "PHASE5_QUALITY_POST_FILTER_WINDOW",
        "phase5_version": "0.3.0", "business_data_written": False,
        "production_release_accepted": False,
        "terminal_records": {
            "record-0": ["PASSED", [], []], "record-1": ["FILTERED", [], []],
            "record-2": ["REJECTED", ["DUPLICATE_CUSTOMER"], []],
            "record-3": ["REJECTED", ["DUPLICATE_CUSTOMER"], []],
        },
        "metrics": metrics(3, 2, {"FILTERED": 1, "PASSED": 1, "REJECTED": 2}),
        "all_filtered_metrics": metrics(0, 0, {"FILTERED": 4, "PASSED": 0, "REJECTED": 0}),
        "threshold_failure": "Quality thresholds blocked: DQ-CUST-001:FAIL_BATCH",
    }


def test_exact_read_only_window_receipt_needs_no_writer_owner():
    value = window_result()
    receipt = {"status": "COMPLETED", "job": {"status": "Completed", "exit_result": value}}
    assert VERIFY["result"](receipt) == value
    VERIFY["verify_window"](value)


@pytest.mark.parametrize("change", ["outcome", "population", "empty", "threshold", "writes", "production", "version"])
def test_window_verifier_rejects_success_shaped_contract_changes(change):
    value = window_result()
    if change == "outcome":
        value["terminal_records"]["record-0"][0] = "REJECTED"
    elif change == "population":
        value["metrics"]["rules"][0]["population"] = 4
    elif change == "empty":
        value["all_filtered_metrics"]["rules"][0]["violations"] = 1
    elif change == "threshold":
        value["threshold_failure"] = ""
    elif change == "writes":
        value["business_data_written"] = True
    elif change == "production":
        value["production_release_accepted"] = True
    else:
        value["phase5_version"] = "0.2.0"
    with pytest.raises(ContractError):
        VERIFY["verify_window"](value)


def sales_source_result():
    return {
        "status": "SUCCEEDED", "scope": "PHASE5_REAL_SALES_SOURCE_SCHEMA", "phase5_version": "0.3.0",
        "source_column_count": 33,
        "source_columns": sorted(VERIFY["SALES_SOURCE_COLUMNS"]),
        "projection_types": {
            "sales_order_detail_id": "bigint", "order_quantity": "int",
            "unit_price": "decimal(18,4)", "unit_price_discount": "decimal(9,6)",
            "total_due_amount": "decimal(18,4)", "order_status_code": "string",
            "source_last_modified_at": "timestamp", "is_deleted": "boolean",
        },
        "original_mapping_failure": "UNRESOLVED_COLUMN.WITH_SUGGESTION",
        "corrected_mapping_expression": "cast(subtotal_amount + tax_amount + freight_amount as decimal(18,4))",
        "authentication": "FABRIC_AUTOMATIC_ENTRA_EXECUTING_IDENTITY",
        "business_rows_extracted": False, "business_data_written": False,
        "production_release_accepted": False,
    }


def test_sales_source_receipt_requires_no_writer_owner_but_exact_schema_contract():
    value = sales_source_result()
    receipt = {"status": "COMPLETED", "job": {"status": "Completed", "exit_result": value}}
    VERIFY["result"](receipt)
    VERIFY["verify_sales_source"](value)


@pytest.mark.parametrize("change", ["rows", "type", "duplicate", "missing", "unexpected", "auth", "failure", "production"])
def test_source_verifier_rejects_success_shaped_contract_changes(change):
    value = sales_source_result()
    if change == "rows":
        value["business_rows_extracted"] = True
    elif change == "type":
        value["projection_types"]["unit_price"] = "double"
    elif change == "duplicate":
        value["source_columns"][-1] = "tax_amount"
    elif change == "missing":
        value["source_columns"][0] = "total_due_amount"
    elif change == "unexpected":
        value["source_columns"][0] = "unexpected_column"
    elif change == "auth":
        value["authentication"] = "UNVERIFIED"
    elif change == "failure":
        value["original_mapping_failure"] = ""
    else:
        value["production_release_accepted"] = True
    with pytest.raises(ContractError):
        VERIFY["verify_sales_source"](value)


def test_cross_version_scalar_check_requires_pinned_outputs_and_preserved_provenance():
    scalar = {
        "accounting": quality_result()["accounting"],
        "fingerprints": {
            "PASSED": "0805bc92cd3f3d10d31e72bed448e3d9e4bfef848480491172c225cb6a450d7d",
            "REJECTED": "37a68e86b86beeed4e680387df0111e54d450a2d9d05e58eec00fb641604310a",
            "FILTERED": "c8ec6b1cb8bfecbb318bcbdccc224c9b5d7194af3d227ef580915d974258fe27",
        },
        "original_commits": {
            "BRONZE": "e085ca73f0d021bf58dcb3043402257d45aff233b9940efe5e87aaa2f32c6b55",
            "QUALITY": "539fae8c54e89b41275dea518f3100c85fd5f8372290df72a0a0883ed49d40a1",
        },
        "targets_unchanged": True, "new_runtime_committed_as_old_runtime": False,
    }
    VERIFY["verify_scalar_regression"]({"scalar_regression": scalar})
    scalar["new_runtime_committed_as_old_runtime"] = True
    with pytest.raises(ContractError):
        VERIFY["verify_scalar_regression"]({"scalar_regression": scalar})


def quality_result():
    def output(name, count):
        return {"target_table": name, "record_count": count, "inserted_count": count,
                "retry_count": 0, "output_fingerprint": name + "-fingerprint"}
    return {
        "scope": "PHASE5_SYNTHETIC_QUALITY_REJECT", "batch_id": "batch",
        "accounting": {
            "bronze_count": 5, "counts": {"FILTERED": 1, "PASSED": 3, "REJECTED": 1},
            "rules": [{"rule_id": name, "threshold_percent": threshold, "action": action,
                       "population": 4, "violations": 1, "violation_percent": 25.0,
                       "breached": False} for name, threshold, action in (
                           ("key_present", 25, "FAIL_BATCH"), ("key_nonblank", 25, "FAIL_BATCH"),
                           ("name_nonblank", 100, "CONTINUE"),
                       )],
        },
        "expected_threshold_failure": "Quality thresholds blocked: key_present:FAIL_BATCH",
        "bronze": output("bronze", 5),
        "outputs": {name: output(name, count) for name, count in (
            ("FILTERED", 1), ("PASSED", 3), ("REJECTED", 1),
        )},
        "bronze_commit": {"commit_id": "bronze-commit", "status": "COMMITTED"},
        "quality_commit": {"commit_id": "quality-commit", "status": "COMMITTED"},
    }


def quality_retry(first):
    retry = copy.deepcopy(first)
    for output in [retry["bronze"], *retry["outputs"].values()]:
        output["inserted_count"] = 0
        output["retry_count"] = output["record_count"]
    for name in ("bronze_commit", "quality_commit"):
        retry[name]["status"] = "VERIFIED"
    return retry


def test_receipt_verifier_accepts_exact_quality_first_and_retry():
    first = quality_result()
    VERIFY["verify_quality"](first, None)
    VERIFY["verify_quality"](first, quality_retry(first))


@pytest.mark.parametrize("change", ["fingerprint", "inserts", "context", "commit", "accounting"])
def test_receipt_verifier_rejects_success_shaped_retry_changes(change):
    first = quality_result()
    retry = quality_retry(first)
    if change == "fingerprint":
        retry["outputs"]["PASSED"]["output_fingerprint"] = "changed"
    elif change == "inserts":
        retry["outputs"]["PASSED"]["inserted_count"] = 1
    elif change == "context":
        retry["batch_id"] = "changed"
    elif change == "commit":
        retry["quality_commit"]["commit_id"] = "changed"
    else:
        retry["accounting"]["counts"]["PASSED"] = 2
    with pytest.raises(ContractError):
        VERIFY["verify_quality"](first, retry)


@pytest.mark.parametrize("change", ["threshold", "population", "distribution"])
def test_receipt_verifier_checks_the_actual_threshold_and_accounting(change):
    first = quality_result()
    if change == "threshold":
        first["expected_threshold_failure"] = ""
    elif change == "population":
        first["accounting"]["rules"][0]["population"] = 5
    else:
        first["accounting"]["counts"] = {"FILTERED": 1, "PASSED": 2, "REJECTED": 2}
    with pytest.raises(ContractError):
        VERIFY["verify_quality"](first, None)


@pytest.mark.parametrize("status", ["SUBMITTED", "FAILED", "TIMEOUT"])
def test_nonterminal_or_failed_receipt_never_proves_success(status):
    with pytest.raises(ContractError):
        VERIFY["result"]({"status": status, "job": {}})


def test_chain_verifier_rejects_missing_deliveries_and_false_production_claim():
    with pytest.raises(ContractError, match="production"):
        VERIFY["verify_chain"]({"scope": "PHASE5_SYNTHETIC_CHAIN_THREE_DELIVERIES",
                               "production_release_accepted": True}, None)
    with pytest.raises(ContractError, match="three"):
        VERIFY["verify_chain"]({"scope": "PHASE5_SYNTHETIC_CHAIN_THREE_DELIVERIES",
                               "production_release_accepted": False, "deliveries": []}, None)


def test_recovery_verifier_requires_all_six_failures_and_unchanged_readback():
    value = {
        "scope": "PHASE5_NEGATIVE_RECOVERY_CONTRACTS",
        "expected_failures": {
            "payload_conflict": "conflicts with deterministic input",
            "stale_owner": "OWNERSHIP_LOST", "out_of_scope": "outside its declared scope",
            "duplicate_key": "key is duplicated", "null_key": "key cannot be NULL",
            "commit_conflict": "conflicts with an existing batch/entity",
        },
        "target_unchanged": True, "commit_unchanged": True, "record_count": 3,
        "output_fingerprint": "0805bc92cd3f3d10d31e72bed448e3d9e4bfef848480491172c225cb6a450d7d",
        "platform_least_privilege_proven": False,
    }
    VERIFY["verify_recovery"](value)
    value["platform_least_privilege_proven"] = True
    with pytest.raises(ContractError, match="least-privilege"):
        VERIFY["verify_recovery"](value)
    value["platform_least_privilege_proven"] = False
    del value["expected_failures"]["null_key"]
    with pytest.raises(ContractError, match="six"):
        VERIFY["verify_recovery"](value)
