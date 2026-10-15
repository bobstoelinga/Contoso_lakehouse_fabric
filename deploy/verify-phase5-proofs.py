"""Verify terminal Fabric receipts against exact probe and cross-job retry contracts."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from datetime import datetime
from pathlib import Path

from fabric_etl.contracts import ContractError, Environment, canonical_json, load_json
from fabric_etl.project import ProjectRelease
from fabric_etl_phase5.mapping import quality_spec


SALES_SOURCE_COLUMNS = {
    "sales_order_detail_id", "sales_order_id", "product_id", "customer_id",
    "bill_to_address_id", "ship_to_address_id", "order_quantity", "unit_price",
    "unit_price_discount", "order_date", "due_date", "ship_date", "order_status_code",
    "is_online_order", "subtotal_amount", "tax_amount", "freight_amount",
    "customer_first_name", "customer_last_name", "bill_to_city", "bill_to_postal_code",
    "ship_to_city", "ship_to_postal_code", "product_name", "product_number", "product_color",
    "product_standard_cost", "product_list_price", "product_category_name",
    "product_model_name", "product_description", "source_last_modified_at", "is_deleted",
}


def result(receipt: dict) -> dict:
    job = receipt.get("job", {})
    value = job.get("exit_result", {})
    read_only_probe = (
        value.get("scope") in {
            "PHASE5_QUALITY_POST_FILTER_WINDOW", "PHASE5_REAL_SALES_SOURCE_SCHEMA",
            "PHASE5_ACTUAL_METADATA_QUALITY_FIXTURES",
        }
        and value.get("business_data_written") is False
    )
    if (
        receipt.get("status") != "COMPLETED" or job.get("status") != "Completed"
        or job.get("failureReason") is not None
        or value.get("status") != "SUCCEEDED"
        or (not read_only_probe and value.get("owner_released") is not True)
    ):
        raise ContractError("A terminal successful Fabric job with released owner is required")
    return value


def verify_sales_quality(value: dict, repeated: dict | None = None) -> None:
    from fabric_etl_phase6.fixtures import sales_fixture
    from fabric_etl_phase6.quality import required_references
    from fabric_etl.contracts import fingerprint

    root = Path(__file__).resolve().parents[1]
    project = ProjectRelease.load(root / "projects" / "contoso" / "release.json")
    environment = Environment.from_dict(load_json(root / "environments" / "contoso.dev.json"))
    sources = {source["id"] for source in project.document["source_objects"] if source["source_system_id"] == "SALES"}
    if (
        value.get("scope") != "PHASE6_SALES_COMMITTED_QUALITY"
        or value.get("phase6_version") != "0.1.0"
        or value.get("release_id") != project.release_id
        or value.get("owner_released") is not True
        or value.get("phase6_complete") is not False
        or value.get("business_data_written") is not False
        or value.get("production_release_accepted") is not False
        or not isinstance(value.get("deliveries"), list)
        or [item.get("delivery_number") for item in value["deliveries"]] != [1, 2, 3]
    ):
        raise ContractError("Sales Quality receipt scope/release/delivery inventory differs")
    batch_ids = [delivery.get("ingest", {}).get("batch_id") for delivery in value["deliveries"]]
    if any(not batch_id for batch_id in batch_ids) or len(set(batch_ids)) != 3:
        raise ContractError("Sales Quality requires three distinct persisted delivery batches")
    timestamps = []
    commit_ids = []
    for delivery in value["deliveries"]:
        number = delivery["delivery_number"]
        ingest = delivery.get("ingest", {})
        quality = delivery.get("quality", {})
        if (
            delivery.get("fixture_fingerprint") != fingerprint(sales_fixture(number))
            or set(quality) != sources or set(ingest.get("outputs", {})) != sources
            or ingest.get("work_status") != "SUCCEEDED" or ingest.get("delivery_status") != "SUCCEEDED"
            or ingest.get("commit", {}).get("status") not in {"COMMITTED", "VERIFIED"}
            or not re.fullmatch(r"[0-9a-f]{64}", ingest.get("audit_id", ""))
            or not re.fullmatch(r"[0-9a-f]{64}", ingest.get("commit", {}).get("commit_id", ""))
            or not ingest.get("batch_id") or not ingest.get("load_date_utc")
            or type(ingest.get("attempt")) is not int or ingest["attempt"] < 1
            or len(ingest.get("files", [])) != 5
            or {file["path"] for file in ingest["files"]} != {
                source.split(".")[1].lower() + ".parquet" for source in sources
            }
            or any(type(file.get("size_bytes")) is not int or file["size_bytes"] <= 0
                   or not re.fullmatch(r"[0-9a-f]{64}", file.get("sha256", "")) for file in ingest["files"])
        ):
            raise ContractError("Sales Quality input scope, physical files or ingest commit is incomplete")
        try:
            loaded = datetime.fromisoformat(ingest["load_date_utc"].replace("Z", "+00:00"))
        except (ValueError, TypeError, AttributeError) as error:
            raise ContractError("Sales Quality requires a persisted UTC load timestamp") from error
        if loaded.tzinfo is None or loaded.utcoffset().total_seconds() != 0:
            raise ContractError("Sales Quality load timestamp must carry UTC context")
        timestamps.append(loaded)
        commit_ids.append(ingest["commit"]["commit_id"])
        for source in sources:
            name = source.split(".")[1].lower()
            bronze = ingest["outputs"][source]
            if (
                bronze["target_table"] != environment.spark_table("processing", "bronze", "phase5_exit_phase6_sales_" + name)
                or bronze["record_count"] != 1
                or bronze["inserted_count"] + bronze["retry_count"] != 1
                or not re.fullmatch(r"[0-9a-f]{64}", bronze["output_fingerprint"])
            ):
                raise ContractError("Sales Bronze accounting or binding differs")
            receipt = quality[source]
            spec = quality_spec(project, source, reference_views={key: "unused" for key in sources})
            if (
                receipt.get("source_id") != source or receipt.get("table") != "phase6_sales_" + name
                or receipt.get("batch_id") != ingest["batch_id"]
                or receipt.get("expected_output_verified") is not True
                or receipt.get("accounting", {}).get("bronze_count") != 1
                or receipt["accounting"].get("counts") != {"PASSED": 1, "REJECTED": 0, "FILTERED": 0}
                or receipt["accounting"].get("release_id") != project.release_id
                or receipt["accounting"].get("source_object_id") != source
                or receipt["accounting"].get("metadata_rule_ids") != spec["metadata_rule_ids"]
                or set(receipt.get("outputs", {})) != {"PASSED", "REJECTED", "FILTERED"}
                or receipt.get("commit", {}).get("status") not in {"COMMITTED", "VERIFIED"}
                or not re.fullmatch(r"[0-9a-f]{64}", receipt.get("commit", {}).get("commit_id", ""))
                or receipt.get("reference_commit_ids") != {
                    key: quality[key]["commit"]["commit_id"] for key in required_references(project, source)
                }
            ):
                raise ContractError("Sales Quality reference provenance/accounting/commit differs")
            commit_ids.append(receipt["commit"]["commit_id"])
            for label, schema, suffix, count in (
                ("PASSED", "quality", "", 1), ("REJECTED", "reject", "", 0),
                ("FILTERED", "quality", "_filtered", 0),
            ):
                output = receipt["outputs"][label]
                if (
                    output["target_table"] != environment.spark_table("processing", schema, "phase6_sales_" + name + suffix)
                    or output["record_count"] != count
                    or type(output["inserted_count"]) is not int or not 0 <= output["inserted_count"] <= count
                    or output["retry_count"] != count - output["inserted_count"]
                    or not re.fullmatch(r"[0-9a-f]{64}", output["output_fingerprint"])
                ):
                    raise ContractError("Sales Quality output count/target/fingerprint differs")
    if any(later <= earlier for earlier, later in zip(timestamps, timestamps[1:])):
        raise ContractError("Sales Quality persisted delivery timestamps must increase")
    if len(set(commit_ids)) != 18:
        raise ContractError("Sales Quality requires eighteen distinct authoritative layer commits")
    if repeated is not None:
        verify_sales_quality(repeated)
        expected = json.loads(json.dumps(value))
        for delivery in expected["deliveries"]:
            ingest = delivery["ingest"]
            ingest["commit"]["status"] = "VERIFIED"
            for output in ingest["outputs"].values():
                output["inserted_count"] = 0
                output["retry_count"] = output["record_count"]
            for receipt in delivery["quality"].values():
                receipt["commit"]["status"] = "VERIFIED"
                for output in receipt["outputs"].values():
                    output["inserted_count"] = 0
                    output["retry_count"] = output["record_count"]
        if repeated != expected:
            raise ContractError("Sales retry changed context, reference commits, values or inserted duplicates")


def verify_sales_vault(value: dict, quality: dict, repeated: dict | None = None) -> None:
    from fabric_etl.contracts import fingerprint
    from fabric_etl_phase6.proof import expected_event_count
    from fabric_etl_phase6.vault import PROJECTION_CONTRACT, vault_order, vault_spec

    verify_sales_quality(quality)
    root = Path(__file__).resolve().parents[1]
    project = ProjectRelease.load(root / "projects" / "contoso" / "release.json")
    environment = Environment.from_dict(load_json(root / "environments" / "contoso.dev.json"))
    raw = vault_order(project)
    business = vault_order(project, business=True)
    entities = raw + business
    if (
        value.get("scope") != "PHASE6_SALES_COMMITTED_VAULT"
        or value.get("status") != "SUCCEEDED"
        or value.get("phase6_version") != "0.2.0"
        or value.get("release_id") != project.release_id
        or value.get("projection_contract") != PROJECTION_CONTRACT
        or value.get("quality_input_fingerprint") != fingerprint(quality)
        or value.get("owner_released") is not True
        or value.get("phase6_complete") is not False
        or value.get("business_data_written") is not False
        or value.get("production_release_accepted") is not False
        or not isinstance(value.get("deliveries"), list)
        or [item.get("delivery_number") for item in value["deliveries"]] != [1, 2, 3]
    ):
        raise ContractError("Sales Vault scope, version or pinned Quality input differs")
    inventories = {entity: [] for entity in entities}
    counts = {entity: 0 for entity in entities}
    commit_ids = []
    for delivery, input_delivery in zip(value["deliveries"], quality["deliveries"]):
        number = delivery["delivery_number"]
        outputs = delivery.get("entities", {})
        if delivery.get("batch_id") != input_delivery["ingest"]["batch_id"] or set(outputs) != set(entities):
            raise ContractError("Sales Vault delivery context or eighteen-entity inventory differs")
        for entity_id in entities:
            receipt = outputs[entity_id]
            spec = vault_spec(project, entity_id, business=entity_id in business)
            expected_inputs = [input_delivery["quality"][spec["source_id"]]["commit"]["commit_id"]] + [
                outputs[parent.split(":", 1)[1]]["commit"]["commit_id"]
                for parent in spec["entity"]["parents"]
            ]
            count = expected_event_count(entity_id, number)
            counts[entity_id] += count
            output = receipt.get("output", {})
            commit = receipt.get("commit", {})
            if (
                receipt.get("entity_id") != entity_id
                or receipt.get("expected_output_verified") is not True
                or receipt.get("input_commit_ids") != expected_inputs
                or receipt.get("history_commit_ids") != inventories[entity_id]
                or receipt.get("history_record_count") != counts[entity_id]
                or commit.get("status") not in {"COMMITTED", "VERIFIED"}
                or not re.fullmatch(r"[0-9a-f]{64}", commit.get("commit_id", ""))
                or output.get("target_table") != environment.spark_table(
                    "processing", "business_vault" if entity_id in business else "raw_vault",
                    "phase6_" + entity_id.lower(),
                )
                or output.get("record_count") != count
                or type(output.get("inserted_count")) is not int
                or not 0 <= output["inserted_count"] <= count
                or output.get("retry_count") != count - output["inserted_count"]
                or not re.fullmatch(r"[0-9a-f]{64}", output.get("output_fingerprint", ""))
            ):
                raise ContractError("Sales Vault payload, event history, input provenance or output commit differs")
            commit_ids.append(commit["commit_id"])
            inventories[entity_id].append(commit["commit_id"])
    if len(set(commit_ids)) != 54:
        raise ContractError("Sales Vault requires fifty-four distinct entity/delivery commits")
    if repeated is not None:
        verify_sales_vault(repeated, quality)
        expected = json.loads(json.dumps(value))
        for delivery in expected["deliveries"]:
            for receipt in delivery["entities"].values():
                receipt["commit"]["status"] = "VERIFIED"
                receipt["output"]["inserted_count"] = 0
                receipt["output"]["retry_count"] = receipt["output"]["record_count"]
        if repeated != expected:
            raise ContractError("Sales Vault retry changed committed history, output values or inserted duplicates")


def verify_sales_gold(value: dict, vault: dict, quality: dict, repeated: dict | None = None) -> None:
    from fabric_etl.contracts import fingerprint
    from fabric_etl_phase6.gold import GOLD_CONTRACT, gold_sql, sales_gold_entities
    from fabric_etl_phase6.pit import PIT_CONTRACT, PIT_PARENTS
    from fabric_etl_phase6.proof import expected_gold_rows

    verify_sales_vault(vault, quality)
    root = Path(__file__).resolve().parents[1]
    project = ProjectRelease.load(root / "projects" / "contoso" / "release.json")
    environment = Environment.from_dict(load_json(root / "environments" / "contoso.dev.json"))
    entities = sales_gold_entities(project)
    sources = {entity["expression_ref"]: (
        root / "projects" / "contoso" / "sql" / (entity["expression_ref"] + ".sql")
    ).read_bytes().decode("utf-8") for entity in entities}
    query_hashes = {entity: hashlib.sha256(query.encode()).hexdigest()
                    for entity, query in gold_sql(project, sources).items()}
    times = [datetime.fromisoformat(delivery["ingest"]["load_date_utc"]) for delivery in quality["deliveries"]]

    def check_output(output: dict, target: str, count: int, *, retry: bool) -> None:
        if (
            output.get("target_table") != target or type(output.get("record_count")) is not int
            or output.get("record_count") != count
            or type(output.get("inserted_count")) is not int
            or not 0 <= output["inserted_count"] <= count
            or output.get("retry_count") != count - output["inserted_count"]
            or not re.fullmatch(r"[0-9a-f]{64}", output.get("output_fingerprint", ""))
            or (retry and output["inserted_count"] != 0)
        ):
            raise ContractError("Sales Gold output binding, count, fingerprint or retry accounting differs")

    for candidate, retry in [(value, False)] + ([(repeated, True)] if repeated is not None else []):
        if (
            candidate.get("status") != "SUCCEEDED" or candidate.get("scope") != "PHASE6_SALES_COMMITTED_GOLD"
            or candidate.get("phase6_version") != "0.3.1" or candidate.get("release_id") != project.release_id
            or candidate.get("gold_contract") != GOLD_CONTRACT or candidate.get("pit_contract") != PIT_CONTRACT
            or candidate.get("vault_input_fingerprint") != fingerprint(vault)
            or candidate.get("owner_released") is not True or candidate.get("phase6_complete") is not False
            or candidate.get("business_data_written") is not False
            or candidate.get("production_release_accepted") is not False or candidate.get("consumer_published") is not False
            or not isinstance(candidate.get("deliveries"), list)
            or [delivery.get("delivery_number") for delivery in candidate["deliveries"]] != [1, 2, 3]
        ):
            raise ContractError("Sales Gold scope, version, complete output or pinned Vault context differs")
        commit_ids = []
        for delivery, input_delivery in zip(candidate["deliveries"], vault["deliveries"]):
            number = delivery["delivery_number"]
            batch_id = input_delivery["batch_id"]
            receipts = input_delivery["entities"]
            expected_inputs = sorted(commit_id for receipt in receipts.values()
                                     for commit_id in [*receipt["history_commit_ids"], receipt["commit"]["commit_id"]])
            pit_inputs = sorted(commit_id for parent in PIT_PARENTS
                                for receipt in [receipts[parent.split(":", 1)[1]]]
                                for commit_id in [*receipt["history_commit_ids"], receipt["commit"]["commit_id"]])
            gold = delivery.get("gold", {})
            pit = delivery.get("pit", {})
            release_id = fingerprint({
                "project_release_id": project.release_id, "input_commits": expected_inputs,
                "batch_id": batch_id, "gold_contract": GOLD_CONTRACT, "query_sha256": query_hashes,
            })
            if (
                delivery.get("batch_id") != batch_id or delivery.get("expected_output_verified") is not True
                or delivery.get("audit_output_verified") is not True
                or gold.get("product_release_id") != release_id or gold.get("input_commit_ids") != expected_inputs
                or gold.get("query_sha256") != query_hashes or gold.get("consumer_published") is not False
                or pit.get("input_commit_ids") != pit_inputs
                or set(gold.get("outputs", {})) != {entity["id"] for entity in entities}
            ):
                raise ContractError("Sales Gold or PIT provenance, exact business/audit evidence or release identity differs")
            for receipt, time_field in ((gold, "cutoff"), (pit, "as_of")):
                try:
                    cutoff = datetime.fromisoformat(receipt[time_field])
                except (KeyError, TypeError, ValueError) as error:
                    raise ContractError("Sales Gold or PIT cutoff is missing or invalid") from error
                if cutoff.tzinfo is None or cutoff.utcoffset().total_seconds() != 0 or cutoff != times[number - 1]:
                    raise ContractError("Sales Gold or PIT cutoff differs from its persisted UTC input batch")
                commit = receipt.get("commit", {})
                if (commit.get("status") not in ({"VERIFIED"} if retry else {"COMMITTED", "VERIFIED"})
                        or not re.fullmatch(r"[0-9a-f]{64}", commit.get("commit_id", ""))):
                    raise ContractError("Sales Gold or PIT commit is missing or not verified")
                commit_ids.append(commit["commit_id"])
            check_output(pit.get("output", {}), environment.spark_table(
                "processing", "business_vault", "phase6_pit_customer",
            ), 1, retry=retry)
            expected = expected_gold_rows(number, times[:number])
            for entity in entities:
                check_output(gold["outputs"][entity["id"]], environment.spark_table(
                    "gold", "historical" if entity["layer"] == "HISTORICAL" else "current",
                    "phase6_" + entity["id"].lower(),
                ), len(expected[entity["id"]]), retry=retry)
            publication = delivery.get("publication", {})
            active_id = candidate["deliveries"][-1]["gold"]["product_release_id"] if retry else release_id
            expected_status = ("VERIFIED" if number == 3 else "VERIFIED_SUPERSEDED") if retry else "ACTIVATED"
            if (publication.get("active_release_id") != active_id
                    or type(publication.get("generation")) is not int
                    or publication.get("generation") != (3 if retry else number)
                    or publication.get("status") != expected_status):
                raise ContractError("Sales Gold active pointer, cutoff progression or stale-retry protection differs")
        if len(set(commit_ids)) != 6:
            raise ContractError("Sales Gold requires six distinct PIT/Gold delivery commits")
    middle = value["deliveries"][1]
    if value.get("recovery") != {
        "attempted": True, "batch_id": middle["batch_id"], "interrupted_after": "GH_DIM_CUSTOMER",
        "previous_active_release_id": value["deliveries"][0]["gold"]["product_release_id"],
        "active_pointer_unchanged": True, "incomplete_commit_absent": True,
        "recovered_gold_commit_id": middle["gold"]["commit"]["commit_id"],
        "recovered_product_release_id": middle["gold"]["product_release_id"],
    }:
        raise ContractError("Sales Gold requires an actual interruption, preserved pointer and successful recovery")
    if repeated is not None:
        expected_retry = json.loads(json.dumps(value))
        expected_retry["recovery"] = {"attempted": False}
        final_id = value["deliveries"][-1]["gold"]["product_release_id"]
        for delivery in expected_retry["deliveries"]:
            for receipt in (delivery["gold"], delivery["pit"]):
                receipt["commit"]["status"] = "VERIFIED"
            for output in [delivery["pit"]["output"], *delivery["gold"]["outputs"].values()]:
                output["inserted_count"] = 0
                output["retry_count"] = output["record_count"]
            delivery["publication"] = {
                "status": "VERIFIED" if delivery["delivery_number"] == 3 else "VERIFIED_SUPERSEDED",
                "active_release_id": final_id, "generation": 3,
            }
        if repeated != expected_retry:
            raise ContractError("Sales Gold retry changed outputs, timelines, audit context or inserted duplicates")


def verify_metadata_write(value: dict, repeated: dict | None = None) -> None:
    root = Path(__file__).resolve().parents[1]
    project = ProjectRelease.load(root / "projects" / "contoso" / "release.json")
    environment = Environment.from_dict(load_json(root / "environments" / "contoso.dev.json"))
    spec = quality_spec(project, "SALES.CUSTOMERS", reference_views={})
    invalid_index = next(index for index, item in enumerate(spec["projections"]) if item["column"] == "is_deleted")
    cast_rule = next(rule for rule in spec["rules"] if rule.expression == f"_mapping_valid_{invalid_index}")
    metrics = [{
        "rule_id": rule.rule_id, "population": 2,
        "violations": 1 if rule.rule_id == cast_rule.rule_id else 0,
        "violation_percent": 50.0 if rule.rule_id == cast_rule.rule_id else 0.0,
        "threshold_percent": rule.max_violation_percent, "breached": False,
        "action": rule.on_threshold_breach,
    } for rule in spec["rules"]]
    expected_accounting = {
        "bronze_count": 2, "counts": {"PASSED": 1, "REJECTED": 1, "FILTERED": 0},
        "rules": metrics, "release_id": project.release_id, "source_object_id": "SALES.CUSTOMERS",
        "metadata_rule_ids": spec["metadata_rule_ids"],
    }
    if (
        value.get("scope") != "PHASE5_METADATA_DURABLE_CUSTOMERS" or value.get("phase5_version") != "0.5.0"
        or value.get("release_id") != project.release_id or value.get("source_id") != "SALES.CUSTOMERS"
        or value.get("production_release_accepted") is not False or value.get("business_data_written") is not False
        or value.get("owner_released") is not True or value.get("accounting") != expected_accounting
        or value.get("reject_error_codes") != [cast_rule.reason_code]
        or not isinstance(value.get("batch_id"), str) or not value["batch_id"]
        or not isinstance(value.get("load_date_utc"), str) or not value["load_date_utc"]
        or set(value.get("outputs", {})) != {"PASSED", "REJECTED", "FILTERED"}
    ):
        raise ContractError("Durable metadata scope, accounting or rejected diagnostic differs")
    for label, role, schema, table, count in (
        ("bronze", "processing", "bronze", "phase5_metadata_customers_v1", 2),
        ("PASSED", "processing", "quality", "phase5_metadata_customers_v1", 1),
        ("REJECTED", "processing", "reject", "phase5_metadata_customers_v1", 1),
        ("FILTERED", "processing", "quality", "phase5_metadata_customers_v1_filtered", 0),
    ):
        output = value.get("bronze", {}) if label == "bronze" else value["outputs"][label]
        if (
            output.get("target_table") != environment.spark_table(role, schema, table)
            or output.get("record_count") != count or output.get("inserted_count") != count
            or output.get("retry_count") != 0
            or not re.fullmatch(r"[0-9a-f]{64}", output.get("output_fingerprint", ""))
        ):
            raise ContractError("Durable metadata first write target/count/fingerprint differs")
    for name in ("bronze_commit", "quality_commit"):
        marker = value.get(name, {})
        if marker.get("status") != "COMMITTED" or not re.fullmatch(r"[0-9a-f]{64}", marker.get("commit_id", "")):
            raise ContractError("Durable metadata first commit is missing")
    if repeated is not None:
        expected = json.loads(json.dumps(value))
        for output in [expected["bronze"], *expected["outputs"].values()]:
            output["inserted_count"] = 0
            output["retry_count"] = output["record_count"]
        expected["bronze_commit"]["status"] = expected["quality_commit"]["status"] = "VERIFIED"
        if repeated != expected:
            raise ContractError("Durable metadata retry must preserve all outputs/commits with zero inserts")


def verify_exit(value: dict, repeated: dict | None = None) -> None:
    environment = Environment.from_dict(load_json(
        Path(__file__).resolve().parents[1] / "environments" / "contoso.dev.json",
    ))
    negative_ids = {
        "schema_JSON", "schema_CSV", "schema_PARQUET", "parquet_type",
        "registered_manifest_conflict", "physical_manifest_conflict", "incomplete_object_scope",
        "physical_file_bytes", "physical_file_missing", "missing_is_not_empty",
    }
    if (
        value.get("scope") != "PHASE5_EXIT_INGESTION_CONTROL" or value.get("phase5_version") != "0.6.1"
        or value.get("owner_released") is not True or value.get("business_data_written") is not False
        or value.get("production_release_accepted") is not False
        or value.get("cross_job_interrupted") is not True
        or set(value.get("negative_cases", {})) != negative_ids | {"older_delivery_blocks_newer"}
        or set(value.get("interruptions", {})) != {"registered", "raw", "bronze", "commit", "status"}
        or set(value.get("deliveries", {})) != {"formats", "registered", "newer", "raw", "bronze", "commit", "status", "empty"}
        or set(value.get("formats", {})) != {"JSON", "CSV", "PARQUET"}
        or value.get("cross_pending", {}).get("commit_count") != 0
        or value.get("cross_pending", {}).get("record_count") != 3
        or value.get("cross_pending", {}).get("delivery_status") != "PROCESSING"
    ):
        raise ContractError("Phase-five exit proof scope/inventory/negative cases differs")
    for name, item in value["formats"].items():
        records = item.get("identity_multiset", [])
        if (
            item.get("count") != 3 or item.get("exact_equal") is not True or item.get("partitions") != [1, 7]
            or len(records) != 3 or sorted(row[2] for row in records) != [1, 1, 2]
            or len({row[0] for row in records}) != 3
            or any(not re.fullmatch(r"[0-9a-f]{64}", row[0]) for row in records)
        ):
            raise ContractError("Phase-five format identity multiset differs: " + name)
    def delivery(name: str, item: dict) -> None:
        if (
            item.get("work_status") != "SUCCEEDED" or item.get("delivery_status") != "SUCCEEDED"
            or item.get("commit", {}).get("status") not in {"COMMITTED", "VERIFIED"}
            or not re.fullmatch(r"[0-9a-f]{64}", item.get("commit", {}).get("commit_id", ""))
            or not re.fullmatch(r"[0-9a-f]{64}", item.get("audit_id", ""))
            or not item.get("batch_id") or not item.get("load_date_utc")
            or type(item.get("attempt")) is not int or item["attempt"] < 1
        ):
            raise ContractError("Phase-five work/commit/audit is incomplete")
        expected = {"EXIT.JSON", "EXIT.CSV", "EXIT.PARQUET"} if name == "formats" else {"EXIT.JSON"}
        if set(item.get("outputs", {})) != expected:
            raise ContractError("Phase-five output object scope differs")
        expected_files = [] if name == "empty" else sorted(
            key.split(".")[1].lower() + "." + key.split(".")[1].lower() for key in expected
        )
        files = item.get("files")
        if (
            not isinstance(files, list) or sorted(file.get("path", "") for file in files) != expected_files
            or any(
                type(file.get("size_bytes")) is not int or file["size_bytes"] <= 0
                or not re.fullmatch(r"[0-9a-f]{64}", file.get("sha256", "")) for file in files
            )
        ):
            raise ContractError("Phase-five physical file register is incomplete")
        case = "stop_" + name if name in {"registered", "raw", "bronze", "commit", "status"} else (
            "newer_blocked" if name == "newer" else name
        )
        for source_id, output in item["outputs"].items():
            count = 0 if name == "empty" else 3
            if (
                output.get("record_count") != count or type(output.get("inserted_count")) is not int
                or not 0 <= output["inserted_count"] <= count
                or output.get("retry_count") != count - output["inserted_count"]
                or not re.fullmatch(r"[0-9a-f]{64}", output.get("output_fingerprint", ""))
                or output.get("target_table") != environment.spark_table(
                    "processing", "bronze", "phase5_exit_" + case + "_" + source_id.split(".")[1].lower(),
                )
            ):
                raise ContractError("Phase-five output accounting differs")
    for name, item in value["deliveries"].items():
        delivery(name, item)
    if repeated is not None:
        if (
            repeated.get("scope") != value["scope"] or repeated.get("phase5_version") != "0.6.1"
            or repeated.get("cross_job_interrupted") is not False or repeated.get("owner_released") is not True
            or repeated.get("business_data_written") is not False or repeated.get("production_release_accepted") is not False
            or repeated.get("formats") != value["formats"]
            or set(repeated.get("deliveries", {})) != set(value["deliveries"]) | {"cross_job"}
            or set(repeated.get("negative_cases", {})) != negative_ids
            or repeated.get("interruptions") != {key: "PREVIOUS_INTERRUPTION_RECOVERED" for key in value["interruptions"]}
            or repeated.get("cross_pending") is not None
        ):
            raise ContractError("Phase-five separate-job retry contract differs")
        for name, item in repeated["deliveries"].items():
            delivery(name, item)
            if any(output["inserted_count"] != 0 for output in item["outputs"].values()):
                raise ContractError("Phase-five retry inserted duplicate records")
            if name != "cross_job":
                expected = json.loads(json.dumps(value["deliveries"][name]))
                expected["commit"]["status"] = "VERIFIED"
                for output in expected["outputs"].values():
                    output["inserted_count"] = 0
                    output["retry_count"] = output["record_count"]
                if item != expected:
                    raise ContractError("Phase-five retry changed context, targets, commits or audit")
        if repeated["deliveries"]["cross_job"]["commit"]["status"] != "COMMITTED":
            raise ContractError("Cross-job recovery did not create its missing commit")
        recovered = repeated["deliveries"]["cross_job"]
        pending = value["cross_pending"]
        output = recovered["outputs"]["EXIT.JSON"]
        if (
            recovered["batch_id"] != pending["batch_id"]
            or any(output[key] != pending[key] for key in ("target_table", "record_count", "output_fingerprint"))
        ):
            raise ContractError("Cross-job recovery changed already-written Bronze")


def verify_metadata_quality(value: dict, repeated: dict | None = None) -> None:
    project = ProjectRelease.load(Path(__file__).resolve().parents[1] / "projects" / "contoso" / "release.json")
    if (
        value.get("scope") != "PHASE5_ACTUAL_METADATA_QUALITY_FIXTURES"
        or value.get("phase5_version") != "0.4.0"
        or value.get("release_id") != project.release_id
        or value.get("business_data_written") is not False
        or value.get("production_release_accepted") is not False
        or value.get("duplicate_threshold_blocked") is not True
        or value.get("retry_fingerprint_equal") is not True
    ):
        raise ContractError("Actual metadata proof scope/release/version/negative contract differs")
    sources = value.get("sources", {})
    expected_ids = {source["id"] for source in project.document["source_objects"]}
    if not isinstance(sources, dict) or set(sources) != expected_ids:
        raise ContractError("Actual metadata proof must cover all nine source objects")
    for source_id, report in sources.items():
        spec = quality_spec(project, source_id, reference_views={key: "reference" for key in expected_ids})
        accounting = report.get("accounting", {})
        expected_metrics = [{
            "rule_id": rule.rule_id, "population": 1, "violations": 0,
            "violation_percent": 0.0, "threshold_percent": rule.max_violation_percent,
            "breached": False, "action": rule.on_threshold_breach,
        } for rule in spec["rules"]]
        if (
            accounting.get("bronze_count") != 1
            or accounting.get("counts") != {"FILTERED": 0, "PASSED": 1, "REJECTED": 0}
            or accounting.get("metadata_rule_ids") != spec["metadata_rule_ids"]
            or accounting.get("rules") != expected_metrics
            or accounting.get("release_id") != project.release_id
            or accounting.get("source_object_id") != source_id
            or not isinstance(report.get("fingerprint"), list)
            or len(report["fingerprint"]) != 2 or report["fingerprint"][0] != 1
            or not isinstance(report["fingerprint"][1], str)
            or not re.fullmatch(r"[0-9a-f]{64}", report["fingerprint"][1])
        ):
            raise ContractError(f"Actual metadata proof output/accounting/rules differ: {source_id}")
    for name, source_id, counts, population in (
        ("ecb_filter_accounting", "ECB.EXCHANGE_RATE", {"FILTERED": 1, "PASSED": 1, "REJECTED": 0}, 1),
        ("invalid_cast_accounting", "SALES.CUSTOMERS", {"FILTERED": 0, "PASSED": 0, "REJECTED": 1}, 1),
    ):
        accounting = value.get(name, {})
        spec = quality_spec(project, source_id, reference_views={})
        rules = accounting.get("rules", [])
        if (
            accounting.get("counts") != counts or accounting.get("bronze_count") != sum(counts.values())
            or accounting.get("source_object_id") != source_id
            or accounting.get("release_id") != project.release_id
            or accounting.get("metadata_rule_ids") != spec["metadata_rule_ids"]
            or [rule.get("rule_id") for rule in rules] != [rule.rule_id for rule in spec["rules"]]
            or any(rule.get("population") != population for rule in rules)
        ):
            raise ContractError("Actual metadata proof filter/cast population differs")
        expected_violations = {
            rule.rule_id: int(name == "invalid_cast_accounting" and rule.rule_id == "CAST-M-QA-CUST-12")
            for rule in spec["rules"]
        }
        if {rule["rule_id"]: rule.get("violations") for rule in rules} != expected_violations:
            raise ContractError("Actual metadata proof cast/eligible violations differ")
    if repeated is not None:
        verify_metadata_quality(repeated)
        if repeated != value:
            raise ContractError("Actual metadata fingerprints/accounting differ across separate jobs")


def verify_window(value: dict) -> None:
    if (
        value.get("scope") != "PHASE5_QUALITY_POST_FILTER_WINDOW"
        or value.get("phase5_version") != "0.3.0"
        or value.get("business_data_written") is not False
        or value.get("production_release_accepted") is not False
    ):
        raise ContractError("Window proof scope/version/write/production contract differs")
    expected_records = {
        "record-0": ["PASSED", [], []], "record-1": ["FILTERED", [], []],
        "record-2": ["REJECTED", ["DUPLICATE_CUSTOMER"], []],
        "record-3": ["REJECTED", ["DUPLICATE_CUSTOMER"], []],
    }
    if value.get("terminal_records") != expected_records:
        raise ContractError("Window proof record identities/outcomes differ")
    for name, population, violations, counts in (
        ("metrics", 3, 2, {"FILTERED": 1, "PASSED": 1, "REJECTED": 2}),
        ("all_filtered_metrics", 0, 0, {"FILTERED": 4, "PASSED": 0, "REJECTED": 0}),
    ):
        expected = {
            "bronze_count": 4, "counts": counts,
            "rules": [{
                "rule_id": "DQ-CUST-001", "population": population, "violations": violations,
                "violation_percent": 100.0 * violations / population if population else 0.0,
                "threshold_percent": 100, "breached": False, "action": "CONTINUE",
            }],
        }
        if value.get(name) != expected:
            raise ContractError("Window proof post-filter population/metrics differ")
    if value.get("threshold_failure") != "Quality thresholds blocked: DQ-CUST-001:FAIL_BATCH":
        raise ContractError("Window proof 66-percent threshold did not block")


def verify_sales_source(value: dict) -> None:
    expected_types = {
        "sales_order_detail_id": "bigint", "order_quantity": "int",
        "unit_price": "decimal(18,4)", "unit_price_discount": "decimal(9,6)",
        "total_due_amount": "decimal(18,4)", "order_status_code": "string",
        "source_last_modified_at": "timestamp", "is_deleted": "boolean",
    }
    columns = value.get("source_columns")
    if (
        value.get("scope") != "PHASE5_REAL_SALES_SOURCE_SCHEMA"
        or value.get("phase5_version") != "0.3.0"
        or value.get("source_column_count") != 33
        or not isinstance(columns, list) or not all(isinstance(name, str) for name in columns)
        or len(columns) != 33 or len(set(columns)) != 33
        or set(columns) != SALES_SOURCE_COLUMNS
        or value.get("projection_types") != expected_types
        or value.get("original_mapping_failure") != "UNRESOLVED_COLUMN.WITH_SUGGESTION"
        or value.get("corrected_mapping_expression")
        != "cast(subtotal_amount + tax_amount + freight_amount as decimal(18,4))"
        or value.get("authentication") != "FABRIC_AUTOMATIC_ENTRA_EXECUTING_IDENTITY"
        or value.get("business_rows_extracted") is not False
        or value.get("business_data_written") is not False
        or value.get("production_release_accepted") is not False
    ):
        raise ContractError("Real Sales source authentication/schema/projection contract differs")


def verify_scalar_regression(value: dict) -> None:
    scalar = value.get("scalar_regression", {})
    expected_fingerprints = {
        "PASSED": "0805bc92cd3f3d10d31e72bed448e3d9e4bfef848480491172c225cb6a450d7d",
        "REJECTED": "37a68e86b86beeed4e680387df0111e54d450a2d9d05e58eec00fb641604310a",
        "FILTERED": "c8ec6b1cb8bfecbb318bcbdccc224c9b5d7194af3d227ef580915d974258fe27",
    }
    expected_commits = {
        "BRONZE": "e085ca73f0d021bf58dcb3043402257d45aff233b9940efe5e87aaa2f32c6b55",
        "QUALITY": "539fae8c54e89b41275dea518f3100c85fd5f8372290df72a0a0883ed49d40a1",
    }
    rules = [
        {"rule_id": name, "population": 4, "violations": 1, "violation_percent": 25.0,
         "threshold_percent": threshold, "breached": False, "action": action}
        for name, threshold, action in (
            ("key_present", 25, "FAIL_BATCH"), ("key_nonblank", 25, "FAIL_BATCH"),
            ("name_nonblank", 100, "CONTINUE"),
        )
    ]
    if (
        scalar.get("fingerprints") != expected_fingerprints
        or scalar.get("original_commits") != expected_commits
        or scalar.get("targets_unchanged") is not True
        or scalar.get("new_runtime_committed_as_old_runtime") is not False
        or scalar.get("accounting") != {
            "bronze_count": 5, "counts": {"FILTERED": 1, "PASSED": 3, "REJECTED": 1},
            "rules": rules,
        }
    ):
        raise ContractError("Cross-version scalar output/commit/accounting regression differs")


def compare_output(first: dict, retry: dict, *, count: int, expect_retry: bool) -> None:
    for name in ("target_table", "record_count", "output_fingerprint"):
        if first[name] != retry[name]:
            raise ContractError(f"Retry changed immutable output: {name}")
    if first["record_count"] != count:
        raise ContractError("Probe output count differs from its fixture contract")
    if expect_retry and (retry["inserted_count"] != 0 or retry["retry_count"] != count):
        raise ContractError("Separate-job retry inserted data or lost retry multiplicity")


def verify_quality(first: dict, retry: dict | None) -> None:
    expected = {"FILTERED": 1, "PASSED": 3, "REJECTED": 1}
    if first.get("scope") != "PHASE5_SYNTHETIC_QUALITY_REJECT":
        raise ContractError("Wrong Quality probe scope")
    if first["accounting"]["bronze_count"] != 5 or first["accounting"]["counts"] != expected:
        raise ContractError("Quality accounting must be exactly 5=1+3+1")
    if first["expected_threshold_failure"] != "Quality thresholds blocked: key_present:FAIL_BATCH":
        raise ContractError("The 24.99-percent threshold did not explicitly block")
    rules = first["accounting"]["rules"]
    expected_rules = {
        "key_present": (25, "FAIL_BATCH"), "key_nonblank": (25, "FAIL_BATCH"),
        "name_nonblank": (100, "CONTINUE"),
    }
    if len(rules) != 3 or {rule["rule_id"] for rule in rules} != set(expected_rules) or any(
        rule["population"] != 4 or rule["violations"] != 1
        or rule["violation_percent"] != 25.0 or rule["breached"]
        or (rule["threshold_percent"], rule["action"]) != expected_rules[rule["rule_id"]]
        for rule in rules
    ):
        raise ContractError("Quality post-filter threshold evidence differs")
    compare_output(first["bronze"], (retry or first)["bronze"], count=5, expect_retry=retry is not None)
    if set(first["outputs"]) != set(expected):
        raise ContractError("Quality target inventory differs")
    for name, count in expected.items():
        compare_output(first["outputs"][name], (retry or first)["outputs"][name],
                       count=count, expect_retry=retry is not None)
    if retry:
        if first["batch_id"] != retry["batch_id"] or first["accounting"] != retry["accounting"]:
            raise ContractError("Quality retry context/accounting changed")
        for name in ("bronze_commit", "quality_commit"):
            if first[name]["commit_id"] != retry[name]["commit_id"] or retry[name]["status"] != "VERIFIED":
                raise ContractError("Quality retry did not verify the identical committed manifest")


def verify_chain(first: dict, retry: dict | None) -> None:
    if first.get("scope") != "PHASE5_SYNTHETIC_CHAIN_THREE_DELIVERIES":
        raise ContractError("Wrong chain probe scope")
    if first.get("production_release_accepted") is not False:
        raise ContractError("Synthetic proof must not claim production release acceptance")
    deliveries = first["deliveries"]
    if len(deliveries) != 3:
        raise ContractError("Chain proof requires exactly three deliveries")
    if retry and len(retry["deliveries"]) != 3:
        raise ContractError("Chain retry lost a delivery")
    previous_time = None
    for index, delivery in enumerate(deliveries):
        current_time = datetime.fromisoformat(delivery["load_date_utc"])
        if current_time.tzinfo is None or (previous_time is not None and current_time <= previous_time):
            raise ContractError("Chain persisted load times must be strictly increasing and timezone-aware")
        previous_time = current_time
        if delivery["delivery_id"] != f"PHASE5_CHAIN_PROBE|{index + 1:02d}":
            raise ContractError("Chain delivery sequence differs")
        other = retry["deliveries"][index] if retry else delivery
        if retry and any(delivery[name] != other[name] for name in (
            "batch_id", "delivery_id", "load_date_utc", "accounting",
        )):
            raise ContractError("Chain retry changed persisted context/accounting")
        bronze_count = (3, 2, 2)[index]
        if delivery["accounting"]["counts"] != {"FILTERED": 0, "PASSED": bronze_count, "REJECTED": 0}:
            raise ContractError("Chain Quality accounting differs")
        compare_output(delivery["bronze"], other["bronze"], count=bronze_count, expect_retry=retry is not None)
        compare_output(delivery["quality"], other["quality"], count=bronze_count, expect_retry=retry is not None)
        layers = {
            "raw_vault": {"customer_hub": (2, 0, 0)[index], "segment_hub": (2, 0, 0)[index],
                          "link": (2, 1, 0)[index], "satellite": (2, 1, 1)[index]},
            "business_vault": {"satellite": (2, 1, 1)[index], "pit": 2},
            "gold": {"historical": (2, 3, 4)[index], "current": (2, 2, 1)[index]},
        }
        for layer, targets in layers.items():
            if set(delivery[layer]) != set(targets):
                raise ContractError("Chain output inventory differs")
            for name, count in targets.items():
                compare_output(delivery[layer][name], other[layer][name],
                               count=count, expect_retry=retry is not None)
        if set(delivery["commits"]) != {"bronze", "quality", "vault", "business_vault", "gold"}:
            raise ContractError("Chain lacks a committed layer")
        for name, commit in delivery["commits"].items():
            if commit["status"] not in {"COMMITTED", "VERIFIED"}:
                raise ContractError("Chain layer is not committed")
            if retry and (commit["commit_id"] != other["commits"][name]["commit_id"]
                          or other["commits"][name]["status"] != "VERIFIED"):
                raise ContractError("Chain retry did not verify the identical layer commit")


def verify_recovery(value: dict) -> None:
    expected = {
        "payload_conflict": "conflicts with deterministic input",
        "stale_owner": "OWNERSHIP_LOST", "out_of_scope": "outside its declared scope",
        "duplicate_key": "key is duplicated", "null_key": "key cannot be NULL",
        "commit_conflict": "conflicts with an existing batch/entity",
    }
    if value.get("scope") != "PHASE5_NEGATIVE_RECOVERY_CONTRACTS":
        raise ContractError("Wrong recovery probe scope")
    failures = value.get("expected_failures", {})
    if set(failures) != set(expected) or any(
        text not in failures[name] for name, text in expected.items()
    ):
        raise ContractError("Recovery must prove all six exact fail-closed paths")
    if (
        value.get("target_unchanged") is not True or value.get("commit_unchanged") is not True
        or value.get("record_count") != 3
        or value.get("output_fingerprint") != "0805bc92cd3f3d10d31e72bed448e3d9e4bfef848480491172c225cb6a450d7d"
        or value.get("platform_least_privilege_proven") is not False
    ):
        raise ContractError("Recovery readback changed, or falsely claimed platform least-privilege proof")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("scope", choices=("quality", "chain", "recovery", "quality-window", "sales-source", "metadata-quality", "metadata-write", "exit", "sales-quality", "sales-vault", "sales-gold"))
    parser.add_argument("first", type=Path)
    parser.add_argument("--retry", type=Path)
    parser.add_argument("--quality-input", type=Path)
    parser.add_argument("--vault-input", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--require-scalar-regression", action="store_true")
    args = parser.parse_args()
    if args.require_scalar_regression and args.scope != "quality-window":
        raise ContractError("Scalar read-only regression is only supported by the window probe")
    if (args.scope in {"sales-vault", "sales-gold"}) != (args.quality_input is not None):
        raise ContractError("Sales Vault/Gold verification requires its explicit Quality input receipt")
    if (args.scope == "sales-gold") != (args.vault_input is not None):
        raise ContractError("Sales Gold verification requires its explicit Vault input receipt")
    initial_receipt = load_json(args.first)
    initial = result(initial_receipt)
    retry_receipt = load_json(args.retry) if args.retry else None
    repeated = result(retry_receipt) if retry_receipt else None
    if args.scope == "quality":
        verify_quality(initial, repeated)
    elif args.scope == "chain":
        verify_chain(initial, repeated)
    elif args.scope == "recovery":
        if args.retry:
            raise ContractError("Recovery verification does not accept a retry receipt")
        verify_recovery(initial)
    elif args.scope == "quality-window":
        if args.retry:
            raise ContractError("Read-only window verification does not accept a retry receipt")
        verify_window(initial)
        if args.require_scalar_regression:
            verify_scalar_regression(initial)
    elif args.scope == "metadata-quality":
        verify_metadata_quality(initial, repeated)
    elif args.scope == "metadata-write":
        verify_metadata_write(initial, repeated)
    elif args.scope == "exit":
        verify_exit(initial, repeated)
    elif args.scope == "sales-quality":
        verify_sales_quality(initial, repeated)
    elif args.scope == "sales-vault":
        verify_sales_vault(initial, result(load_json(args.quality_input)), repeated)
    elif args.scope == "sales-gold":
        verify_sales_gold(initial, result(load_json(args.vault_input)), result(load_json(args.quality_input)), repeated)
    else:
        if args.retry:
            raise ContractError("Read-only source verification does not accept a retry receipt")
        verify_sales_source(initial)
    if retry_receipt and initial_receipt["job"]["id"] == retry_receipt["job"]["id"]:
        raise ContractError("Retry evidence must come from a different Fabric job")
    report = {
        "status": "VERIFIED", "scope": initial["scope"],
        "first_job_id": initial_receipt["job"]["id"],
        "retry_job_id": retry_receipt["job"]["id"] if retry_receipt else None,
        "separate_job_retry_verified": retry_receipt is not None,
        "production_release_accepted": False,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_bytes(canonical_json(report))
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
