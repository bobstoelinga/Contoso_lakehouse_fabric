# Fabric notebook source

# METADATA ********************
# META {
# META   "kernel_info": {"name": "synapse_pyspark"},
# META   "dependencies": {}
# META }

# PARAMETERS CELL ********************
environment_json = "{}"
project_json = "{}"
quality_receipt_json = "{}"
expected_phase6_version = "0.2.0"

# METADATA ********************
# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************
import json
from datetime import datetime, timezone
from uuid import uuid4

import notebookutils
from pyspark.sql import functions as F

from fabric_etl.contracts import Environment, fingerprint
from fabric_etl.control import ControlStore
from fabric_etl.project import ProjectRelease
from fabric_etl_phase5.ingestion import spark_schema
from fabric_etl_phase6 import __version__
from fabric_etl_phase6.proof import expected_event_count, expected_vault_rows
from fabric_etl_phase6.quality import committed_output
from fabric_etl_phase6.vault import (
    PROJECTION_CONTRACT, event_intervals, project_vault, read_history,
    read_vault_commit, vault_order, vault_spec, write_vault,
)

if __version__ != expected_phase6_version:
    raise RuntimeError("Sales Vault proof requires its exact published runtime")
environment = Environment.from_dict(json.loads(environment_json))
project = ProjectRelease(json.loads(project_json), (), ("UNACTIVATED_CANDIDATE",))
quality_receipt = json.loads(quality_receipt_json)
if (quality_receipt["release_id"] != project.release_id
        or quality_receipt["scope"] != "PHASE6_SALES_COMMITTED_QUALITY"
        or quality_receipt["status"] != "SUCCEEDED"):
    raise RuntimeError("Sales Vault proof requires its pinned successful Quality receipt")
spark.conf.set("spark.sql.session.timeZone", "UTC")
control = ControlStore(spark, environment)
owner = control.claim("phase6-sales-vault:" + uuid4().hex, now=datetime.now(timezone.utc))
reports = []
inventories = {key: [] for key in vault_order(project) + vault_order(project, business=True)}
try:
    for delivery in quality_receipt["deliveries"]:
        number = delivery["delivery_number"]
        ingest = delivery["ingest"]
        batch_id = ingest["batch_id"]
        sources = {}
        for source_id, receipt in delivery["quality"].items():
            frame, _ = committed_output(
                control, commit_id=receipt["commit"]["commit_id"], batch_id=batch_id,
                layer="QUALITY", entity=source_id, source_id=source_id,
                target=receipt["outputs"]["PASSED"]["target_table"], role="processing",
                schema="quality", table=receipt["table"], source_system_id="PHASE4_PROOF",
                delivery_id="PHASE6_SALES_QUALITY|" + str(number), release_id=project.release_id,
            )
            schema_id = next(source["schema_id"] for source in project.document["source_objects"]
                             if source["id"] == source_id)
            columns = next(schema["columns"] for schema in project.document["schemas"] if schema["id"] == schema_id)
            sources[source_id] = frame.withColumn(
                "_typed", F.from_json("typed_payload_json", spark_schema(columns), {"mode": "FAILFAST"}),
            ).select("project_id", "batch_id", "load_date_utc", "_typed.*").cache()
        outputs = {}
        try:
            for business in (False, True):
                for entity_id in vault_order(project, business=business):
                    spec = vault_spec(project, entity_id, business=business)
                    expected = expected_vault_rows(number)[entity_id]
                    staged = project_vault(sources[spec["source_id"]], project, entity_id, business=business).cache()
                    try:
                        if set(staged.columns) - {"project_id", "batch_id", "load_date", "record_source", "hashdiff"} != set(expected):
                            raise RuntimeError("Vault projection has missing or extra business columns: " + entity_id)
                        observed = [row.asDict() for row in staged.select(*expected).collect()]
                        if observed != [expected]:
                            raise RuntimeError("Vault projection differs from independent expected values: " + entity_id)
                        empty = staged.limit(0).withColumn("_event_id", F.lit("").cast("string"))
                        previous = read_history(
                            control, project=project, entity_id=entity_id, table="phase6_" + entity_id.lower(),
                            commit_ids=inventories[entity_id], empty=empty, before_batch_id=batch_id, business=business,
                        )
                        inputs = [delivery["quality"][spec["source_id"]]["commit"]["commit_id"]] + [
                            outputs[parent.split(":", 1)[1]]["commit"]["commit_id"]
                            for parent in spec["entity"]["parents"]
                        ]
                        receipt = write_vault(
                            control, owner, staged, previous, project=project, entity_id=entity_id,
                            input_commit_ids=inputs, history_commit_ids=inventories[entity_id],
                            table="phase6_" + entity_id.lower(), now=datetime.now(timezone.utc),
                            business=business, proof_only=True,
                        )
                        if receipt["output"]["record_count"] != expected_event_count(entity_id, number):
                            raise RuntimeError("Vault event count differs from independent change/no-change expectation")
                        current = read_vault_commit(
                            control, project=project, entity_id=entity_id, table="phase6_" + entity_id.lower(),
                            commit_id=receipt["commit"]["commit_id"], batch_id=batch_id, business=business,
                        )
                        history = previous.unionByName(current)
                        if spec["entity"]["type"] not in {"HUB", "LINK"}:
                            intervals = event_intervals(history, key=spec["key_column"])
                            if intervals.where(F.col("is_current")).count() != 1:
                                raise RuntimeError("Satellite must have exactly one current interval per fixture key")
                            if intervals.where(F.col("load_end_date") <= F.col("load_date")).limit(1).count():
                                raise RuntimeError("Satellite interval must be strictly positive")
                        receipt.update(
                            entity_id=entity_id, input_commit_ids=inputs,
                            history_commit_ids=list(inventories[entity_id]), expected_output_verified=True,
                            history_record_count=history.count(),
                        )
                        outputs[entity_id] = receipt
                        inventories[entity_id].append(receipt["commit"]["commit_id"])
                    finally:
                        staged.unpersist()
        finally:
            for source in sources.values():
                source.unpersist()
        reports.append({"delivery_number": number, "batch_id": batch_id, "entities": outputs})
finally:
    control.release(owner)
notebookutils.notebook.exit(json.dumps({
    "status": "SUCCEEDED", "scope": "PHASE6_SALES_COMMITTED_VAULT",
    "phase6_version": __version__, "release_id": project.release_id,
    "projection_contract": PROJECTION_CONTRACT, "quality_input_fingerprint": fingerprint(quality_receipt),
    "deliveries": reports, "owner_released": True, "phase6_complete": False,
    "business_data_written": False, "production_release_accepted": False,
}, sort_keys=True))
