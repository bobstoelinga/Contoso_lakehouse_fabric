import copy
import runpy
from pathlib import Path

import pytest

from fabric_etl.contracts import ContractError, Environment, fingerprint, load_json
from fabric_etl.project import ProjectRelease
from fabric_etl_phase5.mapping import quality_spec
from fabric_etl_phase6.fixtures import sales_fixture
from fabric_etl_phase6.quality import quality_order, required_references


ROOT = Path(__file__).resolve().parents[1]
VERIFY = runpy.run_path(str(ROOT / "deploy" / "verify-phase5-proofs.py"))["verify_sales_quality"]


def proof():
    project = ProjectRelease.load(ROOT / "projects" / "contoso" / "release.json")
    environment = Environment.from_dict(load_json(ROOT / "environments" / "contoso.dev.json"))
    sources = set(sales_fixture(1))
    value = {
        "scope": "PHASE6_SALES_COMMITTED_QUALITY", "phase6_version": "0.1.0",
        "release_id": project.release_id, "owner_released": True,
        "phase6_complete": False, "business_data_written": False,
        "production_release_accepted": False, "deliveries": [],
    }
    for number in (1, 2, 3):
        batch_id = "batch-" + str(number)
        quality = {}
        ingest = {
            "batch_id": batch_id, "load_date_utc": f"2026-10-12T06:00:0{number}+00:00",
            "commit": {"status": "COMMITTED", "commit_id": fingerprint(["INGEST", number])},
            "audit_id": fingerprint(["AUDIT", number]), "attempt": 1,
            "work_status": "SUCCEEDED", "delivery_status": "SUCCEEDED", "outputs": {},
            "files": [
                {"path": source.split(".")[1].lower() + ".parquet", "size_bytes": 3,
                 "sha256": fingerprint([source, number])} for source in sorted(sources)
            ],
        }
        for source in quality_order(project, sources):
            name = source.split(".")[1].lower()
            ingest["outputs"][source] = {
                "target_table": environment.spark_table("processing", "bronze", "phase5_exit_phase6_sales_" + name),
                "record_count": 1, "inserted_count": 1, "retry_count": 0,
                "output_fingerprint": fingerprint(["BRONZE", source, number]),
            }
            spec = quality_spec(project, source, reference_views={key: "unused" for key in sources})
            quality[source] = {
                "source_id": source, "table": "phase6_sales_" + name, "batch_id": batch_id,
                "expected_output_verified": True,
                "accounting": {
                    "bronze_count": 1, "counts": {"PASSED": 1, "REJECTED": 0, "FILTERED": 0},
                    "release_id": project.release_id, "source_object_id": source,
                    "metadata_rule_ids": spec["metadata_rule_ids"],
                },
                "commit": {"status": "COMMITTED", "commit_id": fingerprint(["QUALITY", source, number])},
                "reference_commit_ids": {
                    key: quality[key]["commit"]["commit_id"] for key in required_references(project, source)
                },
                "outputs": {
                    label: {
                        "target_table": environment.spark_table("processing", schema, "phase6_sales_" + name + suffix),
                        "record_count": count, "inserted_count": count, "retry_count": 0,
                        "output_fingerprint": fingerprint([label, source, number]),
                    } for label, schema, suffix, count in (
                        ("PASSED", "quality", "", 1), ("REJECTED", "reject", "", 0),
                        ("FILTERED", "quality", "_filtered", 0),
                    )
                },
            }
        value["deliveries"].append({
            "delivery_number": number, "fixture_fingerprint": fingerprint(sales_fixture(number)),
            "ingest": ingest, "quality": quality,
        })
    repeated = copy.deepcopy(value)
    for delivery in repeated["deliveries"]:
        for receipt in [delivery["ingest"], *delivery["quality"].values()]:
            receipt["commit"]["status"] = "VERIFIED"
            for output in receipt["outputs"].values():
                output["inserted_count"] = 0
                output["retry_count"] = output["record_count"]
    return value, repeated


def test_sales_receipts_require_exact_three_delivery_reference_and_zero_insert_retry_contract():
    first, repeated = proof()
    VERIFY(first, repeated)


@pytest.mark.parametrize("change", [
    "full_phase_claim", "input_bytes", "fixture", "accounting", "reference", "scope",
    "binding", "expected_values", "retry_insert", "retry_timestamp", "same_batch",
    "timestamp_order", "timestamp_timezone", "same_commit",
])
def test_sales_receipts_reject_success_shaped_partial_or_changed_output(change):
    first, repeated = proof()
    delivery = first["deliveries"][0]
    quality = delivery["quality"]["SALES.ORDERS"]
    if change == "full_phase_claim":
        first["phase6_complete"] = True
    elif change == "input_bytes":
        delivery["ingest"]["files"][0]["size_bytes"] = 0
    elif change == "fixture":
        delivery["fixture_fingerprint"] = "0" * 64
    elif change == "accounting":
        quality["accounting"]["counts"]["PASSED"] = 2
    elif change == "reference":
        quality["reference_commit_ids"]["SALES.PRODUCTS"] = "0" * 64
    elif change == "scope":
        del delivery["quality"]["SALES.EMPLOYEES"]
    elif change == "binding":
        quality["outputs"]["PASSED"]["target_table"] = "foreign_table"
    elif change == "expected_values":
        quality["expected_output_verified"] = False
    elif change == "retry_insert":
        repeated["deliveries"][0]["quality"]["SALES.ORDERS"]["outputs"]["PASSED"]["inserted_count"] = 1
    elif change == "retry_timestamp":
        repeated["deliveries"][0]["ingest"]["load_date_utc"] = "2026-10-12T06:00:00+00:00"
    elif change == "same_batch":
        first["deliveries"][1]["ingest"]["batch_id"] = delivery["ingest"]["batch_id"]
    elif change == "timestamp_order":
        first["deliveries"][1]["ingest"]["load_date_utc"] = delivery["ingest"]["load_date_utc"]
    elif change == "timestamp_timezone":
        delivery["ingest"]["load_date_utc"] = "2026-10-12T06:00:00"
    else:
        delivery["quality"]["SALES.EMPLOYEES"]["commit"]["commit_id"] = quality["commit"]["commit_id"]
    with pytest.raises(ContractError):
        VERIFY(first, repeated)
