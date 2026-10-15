import copy
import hashlib
import runpy
from datetime import datetime
from pathlib import Path

import pytest

from fabric_etl.contracts import ContractError, Environment, fingerprint, load_json
from fabric_etl_phase6.gold import GOLD_CONTRACT, gold_sql, sales_gold_entities
from fabric_etl_phase6.pit import PIT_CONTRACT, PIT_PARENTS
from fabric_etl_phase6.proof import expected_gold_rows
from test_fabric_sales_gold import sources
from test_fabric_sales_vault import project
from test_fabric_sales_vault_receipts import proof as vault_proof


ROOT = Path(__file__).resolve().parents[1]
VERIFY = runpy.run_path(str(ROOT / "deploy" / "verify-phase5-proofs.py"))["verify_sales_gold"]


def proof():
    vault, quality, _ = vault_proof()
    release = project()
    environment = Environment.from_dict(load_json(ROOT / "environments" / "contoso.dev.json"))
    entities = sales_gold_entities(release)
    hashes = {entity: hashlib.sha256(query.encode()).hexdigest()
              for entity, query in gold_sql(release, sources()).items()}
    times = [datetime.fromisoformat(delivery["ingest"]["load_date_utc"]) for delivery in quality["deliveries"]]
    value = {
        "status": "SUCCEEDED", "scope": "PHASE6_SALES_COMMITTED_GOLD", "phase6_version": "0.3.1",
        "release_id": release.release_id, "gold_contract": GOLD_CONTRACT, "pit_contract": PIT_CONTRACT,
        "vault_input_fingerprint": fingerprint(vault), "owner_released": True, "phase6_complete": False,
        "business_data_written": False, "production_release_accepted": False,
        "consumer_published": False, "deliveries": [],
    }

    def output(target, count, entity, number):
        return {
            "target_table": target, "record_count": count, "inserted_count": count, "retry_count": 0,
            "output_fingerprint": fingerprint(["fixture-output", entity, number]),
        }

    for delivery in vault["deliveries"]:
        number = delivery["delivery_number"]
        receipts = delivery["entities"]
        inputs = sorted(commit for receipt in receipts.values()
                        for commit in [*receipt["history_commit_ids"], receipt["commit"]["commit_id"]])
        release_id = fingerprint({
            "project_release_id": release.release_id, "input_commits": inputs,
            "batch_id": delivery["batch_id"], "gold_contract": GOLD_CONTRACT, "query_sha256": hashes,
        })
        expected = expected_gold_rows(number, times[:number])
        gold = {
            "product_release_id": release_id, "input_commit_ids": inputs, "query_sha256": hashes,
            "consumer_published": False, "cutoff": times[number - 1].isoformat(),
            "commit": {"status": "COMMITTED", "commit_id": fingerprint(["GOLD", number])},
            "outputs": {
                entity["id"]: output(environment.spark_table(
                    "gold", "historical" if entity["layer"] == "HISTORICAL" else "current",
                    "phase6_" + entity["id"].lower(),
                ), len(expected[entity["id"]]), entity["id"], number)
                for entity in entities
            },
        }
        pit = {
            "as_of": times[number - 1].isoformat(),
            "input_commit_ids": sorted(commit for parent in PIT_PARENTS
                                      for receipt in [receipts[parent.split(":", 1)[1]]]
                                      for commit in [*receipt["history_commit_ids"], receipt["commit"]["commit_id"]]),
            "commit": {"status": "COMMITTED", "commit_id": fingerprint(["PIT", number])},
            "output": output(environment.spark_table(
                "processing", "business_vault", "phase6_pit_customer",
            ), 1, "PIT_CUSTOMER", number),
        }
        value["deliveries"].append({
            "delivery_number": number, "batch_id": delivery["batch_id"], "pit": pit, "gold": gold,
            "expected_output_verified": True, "audit_output_verified": True,
            "publication": {"status": "ACTIVATED", "active_release_id": release_id, "generation": number},
        })
    middle = value["deliveries"][1]
    value["recovery"] = {
        "attempted": True, "batch_id": middle["batch_id"], "interrupted_after": "GH_DIM_CUSTOMER",
        "previous_active_release_id": value["deliveries"][0]["gold"]["product_release_id"],
        "active_pointer_unchanged": True, "incomplete_commit_absent": True,
        "recovered_gold_commit_id": middle["gold"]["commit"]["commit_id"],
        "recovered_product_release_id": middle["gold"]["product_release_id"],
    }
    retry = copy.deepcopy(value)
    retry["recovery"] = {"attempted": False}
    final_id = value["deliveries"][-1]["gold"]["product_release_id"]
    for delivery in retry["deliveries"]:
        for receipt in (delivery["pit"], delivery["gold"]):
            receipt["commit"]["status"] = "VERIFIED"
        for target in [delivery["pit"]["output"], *delivery["gold"]["outputs"].values()]:
            target["inserted_count"] = 0
            target["retry_count"] = target["record_count"]
        delivery["publication"] = {
            "status": "VERIFIED" if delivery["delivery_number"] == 3 else "VERIFIED_SUPERSEDED",
            "active_release_id": final_id, "generation": 3,
        }
    return value, vault, quality, retry


def test_gold_receipts_require_exact_outputs_six_commits_recovery_and_no_rollback_on_separate_retry():
    first, vault, quality, retry = proof()
    VERIFY(first, vault, quality, retry)


@pytest.mark.parametrize("change", [
    "phase", "version", "vault", "missing", "extra", "count", "bool_count", "binding", "fingerprint",
    "provenance", "query", "release", "cutoff", "timezone", "pit_input", "pit_cutoff", "pit_count",
    "audit", "values", "pointer", "generation", "recovery", "recovery_pointer", "recovery_commit",
    "retry_insert", "retry_fingerprint", "retry_pointer", "retry_audit", "duplicate_commit",
])
def test_gold_receipts_reject_incomplete_or_success_shaped_mismatches(change):
    first, vault, quality, retry = proof()
    delivery = first["deliveries"][1]
    gold = delivery["gold"]
    output = gold["outputs"]["GH_DIM_CUSTOMER"]
    if change == "phase":
        first["phase6_complete"] = True
    elif change == "version":
        first["phase6_version"] = "0.2.0"
    elif change == "vault":
        first["vault_input_fingerprint"] = "foreign"
    elif change == "missing":
        del gold["outputs"]["GC_DIM_DATE"]
    elif change == "extra":
        gold["outputs"]["FOREIGN"] = output
    elif change in {"count", "bool_count"}:
        output["record_count"] = True if change == "bool_count" else 99
    elif change == "binding":
        output["target_table"] = "foreign"
    elif change == "fingerprint":
        output["output_fingerprint"] = "invalid"
    elif change == "provenance":
        gold["input_commit_ids"] = []
    elif change == "query":
        gold["query_sha256"]["GH_FCT_SALES"] = fingerprint("foreign-query")
    elif change == "release":
        gold["product_release_id"] = fingerprint("foreign-release")
    elif change in {"cutoff", "timezone"}:
        gold["cutoff"] = "2026-10-12T06:00:01+00:00" if change == "cutoff" else "2026-10-12T06:00:02"
    elif change == "pit_input":
        delivery["pit"]["input_commit_ids"] = []
    elif change == "pit_cutoff":
        delivery["pit"]["as_of"] = "2026-10-12T06:00:01+00:00"
    elif change == "pit_count":
        delivery["pit"]["output"]["record_count"] = 0
    elif change in {"audit", "values"}:
        delivery["audit_output_verified" if change == "audit" else "expected_output_verified"] = False
    elif change == "pointer":
        delivery["publication"]["active_release_id"] = first["deliveries"][0]["gold"]["product_release_id"]
    elif change == "generation":
        delivery["publication"]["generation"] = 99
    elif change == "recovery":
        first["recovery"] = {"attempted": False}
    elif change == "recovery_pointer":
        first["recovery"]["active_pointer_unchanged"] = False
    elif change == "recovery_commit":
        first["recovery"]["incomplete_commit_absent"] = False
    elif change == "retry_insert":
        retry["deliveries"][0]["gold"]["outputs"]["GH_DIM_CUSTOMER"]["inserted_count"] = 1
    elif change == "retry_fingerprint":
        retry["deliveries"][0]["gold"]["outputs"]["GH_DIM_CUSTOMER"]["output_fingerprint"] = fingerprint("changed")
    elif change == "retry_pointer":
        retry["deliveries"][0]["publication"]["active_release_id"] = first["deliveries"][0]["gold"]["product_release_id"]
    elif change == "retry_audit":
        retry["deliveries"][0]["gold"]["cutoff"] = "2026-10-12T06:00:00+00:00"
    else:
        delivery["pit"]["commit"]["commit_id"] = first["deliveries"][0]["pit"]["commit"]["commit_id"]
    with pytest.raises(ContractError):
        VERIFY(first, vault, quality, retry)
