# Fabric notebook source

# METADATA ********************
# META {
# META   "kernel_info": {"name": "synapse_pyspark"},
# META   "dependencies": {}
# META }

# PARAMETERS CELL ********************
environment_json = "{}"
project_json = "{}"
vault_receipt_json = "{}"
gold_sources_json = "{}"
expected_phase6_version = "0.3.1"

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

from fabric_etl.contracts import ContractError, Environment, fingerprint
from fabric_etl.control import ControlStore, literal, rows
from fabric_etl.project import ProjectRelease
from fabric_etl_phase5.ingestion import spark_schema
from fabric_etl_phase6 import __version__
from fabric_etl_phase6.gold import GOLD_CONTRACT, gold_columns, sales_gold_entities, write_gold
from fabric_etl_phase6.pit import PIT_CONTRACT, committed_state, write_pit
from fabric_etl_phase6.proof import expected_gold_rows
from fabric_etl_phase6.publication import PRODUCT_ID, publish_gold
from fabric_etl_phase6.vault import _proof_batch

if __version__ != expected_phase6_version:
    raise RuntimeError("Sales Gold proof requires its exact published runtime")
environment = Environment.from_dict(json.loads(environment_json))
project = ProjectRelease(json.loads(project_json), (), ("UNACTIVATED_CANDIDATE",))
vault_receipt = json.loads(vault_receipt_json)
sources = json.loads(gold_sources_json)
if (vault_receipt.get("release_id") != project.release_id
        or vault_receipt.get("scope") != "PHASE6_SALES_COMMITTED_VAULT"
        or vault_receipt.get("status") != "SUCCEEDED"):
    raise RuntimeError("Sales Gold proof requires its pinned successful Vault receipt")
deliveries = vault_receipt["deliveries"]
if [delivery["delivery_number"] for delivery in deliveries] != [1, 2, 3]:
    raise RuntimeError("Sales Gold proof requires its exact ordered three-delivery input")
spark.conf.set("spark.sql.session.timeZone", "UTC")
control = ControlStore(spark, environment)
owner = control.claim("phase6-sales-gold:" + uuid4().hex, now=datetime.now(timezone.utc))
reports = []
load_dates = []
recovery = {"attempted": False}
try:
    control.assert_owner(owner)
    current_schema = environment.spark_table("gold", "current", "phase6_gc_dim_date").rsplit(".", 1)[0]
    spark.sql("CREATE SCHEMA IF NOT EXISTS " + current_schema).collect()
    for delivery in deliveries:
        number = delivery["delivery_number"]
        batch_id = delivery["batch_id"]
        batch = _proof_batch(control, project, batch_id)
        cutoff = batch["load_date"]
        cutoff = cutoff.replace(tzinfo=timezone.utc) if cutoff.tzinfo is None else cutoff
        load_dates.append(cutoff)
        pit_receipt = write_pit(
            control, owner, project=project, delivery=delivery,
            now=datetime.now(timezone.utc), proof_only=True,
        )
        pit = spark.table(pit_receipt["output"]["target_table"]).where(
            (F.col("project_id") == control.project) & (F.col("batch_id") == batch_id),
        )
        observed_pit = pit.collect()
        if len(observed_pit) != 1 or observed_pit[0]["as_of"] != cutoff.replace(tzinfo=None):
            raise RuntimeError("PIT output differs from the persisted cutoff or expected fixture grain")
        for label, entity_id in (("raw", "SAT_CUSTOMER"), ("bv", "SAT_CUSTOMER_BV")):
            state = committed_state(
                control, project=project, entity_id=entity_id,
                receipt=delivery["entities"][entity_id], batch_id=batch_id,
            )
            latest = state.where(F.col("load_date") <= F.lit(cutoff)).orderBy(
                F.col("load_date").desc(),
            ).first()
            if (latest is None or observed_pit[0][label + "_event_id"] != latest["_event_id"]
                    or observed_pit[0][label + "_load_date"] != latest["load_date"]
                    or observed_pit[0]["hk_customer"] != latest["hk_customer"]):
                raise RuntimeError("PIT pointer differs from its latest committed as-of satellite event")
        predicate = (
            f"batch_id = {literal(batch_id)} "
            f"AND layer = 'GOLD' AND entity_id = {literal(PRODUCT_ID)} AND status = 'COMMITTED'"
        )
        existing = rows(control.spark, f"SELECT * FROM {control.table('commit_manifest')} WHERE {predicate}")
        if number == 2 and not existing:
            active_table = environment.spark_table("gold", "publication", "active_product")
            active_predicate = f"project_id = {literal(control.project)} AND product_id = {literal(PRODUCT_ID)}"
            before = rows(control.spark, f"SELECT * FROM {active_table} WHERE {active_predicate}")
            if len(before) != 1:
                raise RuntimeError("Controlled Gold recovery requires one previously good proof pointer")
            try:
                write_gold(
                    control, owner, project=project, delivery=delivery, sources=sources,
                    now=datetime.now(timezone.utc), proof_only=True, stop_after_entity="GH_DIM_CUSTOMER",
                )
            except ContractError as error:
                if str(error) != "Controlled Gold interruption after immutable output: GH_DIM_CUSTOMER":
                    raise
            else:
                raise RuntimeError("Requested Gold interruption did not occur")
            after = rows(control.spark, f"SELECT * FROM {active_table} WHERE {active_predicate}")
            incomplete = rows(control.spark, f"SELECT * FROM {control.table('commit_manifest')} WHERE {predicate}")
            if after != before or incomplete:
                raise RuntimeError("Interrupted Gold build changed the active pointer or committed an incomplete product")
            recovery = {
                "attempted": True, "batch_id": batch_id, "interrupted_after": "GH_DIM_CUSTOMER",
                "previous_active_release_id": before[0]["active_release_id"],
                "active_pointer_unchanged": True, "incomplete_commit_absent": True,
            }
        gold = write_gold(
            control, owner, project=project, delivery=delivery, sources=sources,
            now=datetime.now(timezone.utc), proof_only=True,
        )
        if number == 2 and recovery["attempted"]:
            recovery["recovered_gold_commit_id"] = gold["commit"]["commit_id"]
            recovery["recovered_product_release_id"] = gold["product_release_id"]
        expected = expected_gold_rows(number, load_dates)
        for entity in sales_gold_entities(project):
            entity_id = entity["id"]
            output = gold["outputs"][entity_id]
            persisted = spark.table(output["target_table"]).where(
                (F.col("project_id") == control.project) & (F.col("batch_id") == batch_id),
            )
            columns = gold_columns(project, entity, business_only=True)
            values = [
                {key: value.replace(tzinfo=timezone.utc) if isinstance(value, datetime) and value.tzinfo is None
                 else value for key, value in row.items()}
                for row in expected[entity_id]
            ]
            expected_frame = spark.createDataFrame(values, spark_schema(columns))
            actual = persisted.select(*[column["name"] for column in columns])
            if (actual.exceptAll(expected_frame).limit(1).count()
                    or expected_frame.exceptAll(actual).limit(1).count()):
                raise RuntimeError("Gold business output differs from independent exact expectations: " + entity_id)
            invalid_audit = F.col("_batch_id").isNull() | (F.col("_batch_id") != batch_id)
            if entity["layer"] == "HISTORICAL":
                invalid_audit = invalid_audit | F.col("_loaded_at").isNull() | (F.col("_loaded_at") != F.lit(cutoff))
            else:
                invalid_audit = (
                    invalid_audit | F.col("_as_of_delivery_id").isNull()
                    | (F.col("_as_of_delivery_id") != batch["delivery_id"])
                    | F.col("_as_of_timestamp").isNull() | (F.col("_as_of_timestamp") != F.lit(cutoff))
                )
            if persisted.where(invalid_audit).limit(1).count():
                raise RuntimeError("Gold audit values differ from the persisted proof batch: " + entity_id)
        publication = publish_gold(
            control, owner, project=project, receipt=gold, batch_id=batch_id,
            now=datetime.now(timezone.utc), proof_only=True,
        )
        reports.append({
            "delivery_number": number, "batch_id": batch_id, "pit": pit_receipt, "gold": gold,
            "publication": publication, "expected_output_verified": True, "audit_output_verified": True,
        })
finally:
    control.release(owner)
notebookutils.notebook.exit(json.dumps({
    "status": "SUCCEEDED", "scope": "PHASE6_SALES_COMMITTED_GOLD",
    "phase6_version": __version__, "release_id": project.release_id,
    "gold_contract": GOLD_CONTRACT, "pit_contract": PIT_CONTRACT,
    "vault_input_fingerprint": fingerprint(vault_receipt),
    "deliveries": reports, "recovery": recovery, "owner_released": True,
    "phase6_complete": False, "business_data_written": False,
    "production_release_accepted": False, "consumer_published": False,
}, sort_keys=True))
