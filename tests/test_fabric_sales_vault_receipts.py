import copy
import runpy
from pathlib import Path

import pytest

from fabric_etl.contracts import ContractError, Environment, fingerprint, load_json
from fabric_etl_phase6.proof import expected_event_count
from fabric_etl_phase6.vault import PROJECTION_CONTRACT, vault_order, vault_spec
from test_fabric_sales_receipts import proof as quality_proof
from test_fabric_sales_vault import project


ROOT = Path(__file__).resolve().parents[1]
VERIFY = runpy.run_path(str(ROOT / "deploy" / "verify-phase5-proofs.py"))["verify_sales_vault"]


def proof():
    quality, _ = quality_proof()
    release = project()
    environment = Environment.from_dict(load_json(ROOT / "environments" / "contoso.dev.json"))
    entities = vault_order(release) + vault_order(release, business=True)
    inventories = {entity: [] for entity in entities}
    counts = {entity: 0 for entity in entities}
    value = {
        "status": "SUCCEEDED", "scope": "PHASE6_SALES_COMMITTED_VAULT", "phase6_version": "0.2.0",
        "release_id": release.release_id, "projection_contract": PROJECTION_CONTRACT,
        "quality_input_fingerprint": fingerprint(quality), "owner_released": True,
        "phase6_complete": False, "business_data_written": False, "production_release_accepted": False,
        "deliveries": [],
    }
    for delivery in quality["deliveries"]:
        number = delivery["delivery_number"]
        outputs = {}
        for entity in entities:
            spec = vault_spec(release, entity, business=entity.endswith("_BV"))
            count = expected_event_count(entity, number)
            counts[entity] += count
            commit_id = fingerprint([entity, number])
            outputs[entity] = {
                "entity_id": entity, "expected_output_verified": True,
                "input_commit_ids": [delivery["quality"][spec["source_id"]]["commit"]["commit_id"]] + [
                    outputs[parent.split(":", 1)[1]]["commit"]["commit_id"]
                    for parent in spec["entity"]["parents"]
                ],
                "history_commit_ids": list(inventories[entity]), "history_record_count": counts[entity],
                "commit": {"status": "COMMITTED", "commit_id": commit_id},
                "output": {
                    "target_table": environment.spark_table(
                        "processing", "business_vault" if entity.endswith("_BV") else "raw_vault",
                        "phase6_" + entity.lower(),
                    ),
                    "record_count": count, "inserted_count": count, "retry_count": 0,
                    "output_fingerprint": fingerprint(["OUTPUT", entity, number]),
                },
            }
            inventories[entity].append(commit_id)
        value["deliveries"].append({
            "delivery_number": number, "batch_id": delivery["ingest"]["batch_id"], "entities": outputs,
        })
    retry = copy.deepcopy(value)
    for delivery in retry["deliveries"]:
        for receipt in delivery["entities"].values():
            receipt["commit"]["status"] = "VERIFIED"
            receipt["output"]["inserted_count"] = 0
            receipt["output"]["retry_count"] = receipt["output"]["record_count"]
    return value, quality, retry


def test_vault_receipts_require_fifty_four_commits_and_exact_retry_and_no_change_events():
    first, quality, repeated = proof()
    VERIFY(first, quality, repeated)


@pytest.mark.parametrize("change", [
    "phase_claim", "version", "input", "scope", "parent", "history", "count",
    "no_change", "binding", "expected", "timestamp_context", "commit", "retry_insert", "retry_hash",
])
def test_vault_receipts_reject_incomplete_or_changed_success_shaped_proof(change):
    first, quality, repeated = proof()
    receipt = first["deliveries"][1]["entities"]["SAT_CUSTOMER"]
    if change == "phase_claim":
        first["phase6_complete"] = True
    elif change == "version":
        first["phase6_version"] = "0.1.0"
    elif change == "input":
        first["quality_input_fingerprint"] = "0" * 64
    elif change == "scope":
        del first["deliveries"][0]["entities"]["HUB_PRODUCT"]
    elif change == "parent":
        receipt["input_commit_ids"][-1] = "0" * 64
    elif change == "history":
        receipt["history_commit_ids"] = []
    elif change == "count":
        receipt["history_record_count"] = 1
    elif change == "no_change":
        first["deliveries"][1]["entities"]["SAT_ORDER"]["output"]["record_count"] = 1
    elif change == "binding":
        receipt["output"]["target_table"] = "foreign"
    elif change == "expected":
        receipt["expected_output_verified"] = False
    elif change == "timestamp_context":
        first["deliveries"][1]["batch_id"] = first["deliveries"][0]["batch_id"]
    elif change == "commit":
        receipt["commit"]["commit_id"] = "wrong"
    elif change == "retry_insert":
        repeated["deliveries"][1]["entities"]["SAT_CUSTOMER"]["output"]["inserted_count"] = 1
    else:
        repeated["deliveries"][1]["entities"]["SAT_CUSTOMER"]["output"]["output_fingerprint"] = "0" * 64
    with pytest.raises(ContractError):
        VERIFY(first, quality, repeated)
