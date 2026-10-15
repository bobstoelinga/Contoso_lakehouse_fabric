# Fabric notebook source

# METADATA ********************
# META {
# META   "kernel_info": {"name": "synapse_pyspark"},
# META   "dependencies": {}
# META }

# PARAMETERS CELL ********************
environment_json = "{}"
expected_framework_version = "0.1.0"
expected_phase5_version = "0.2.0"

# METADATA ********************
# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************
import hashlib
import json
from datetime import datetime, timezone
from uuid import uuid4

import notebookutils
from pyspark.sql import Window, functions as F

from fabric_etl import __version__ as framework_version
from fabric_etl.context import BatchContext
from fabric_etl.contracts import Environment, canonical_json, fingerprint, hash_key
from fabric_etl.control import ControlStore, literal, rows
from fabric_etl.delivery import Manifest
from fabric_etl_phase5 import __version__ as phase5_version
from fabric_etl_phase5.ingestion import (
    read_manifest_records, record_bronze_commit, spark_schema,
    validate_file_inventory, write_bronze_delivery,
)
from fabric_etl_phase5.quality import Rule, commit_layer, evaluate_quality, write_immutable_output

SOURCE_SYSTEM_ID = "PHASE4_PROOF"
SOURCE_OBJECT_ID = "PROBE.CHAIN_CUSTOMERS"
RELEASE_ID = "phase5-chain-probe-v1"
FIXTURES = [
    [
        {"customer_id": "CHAIN-001", "display_name": "Alice", "segment": "GOLD", "is_deleted": False},
        {"customer_id": "CHAIN-001", "display_name": "Alice", "segment": "GOLD", "is_deleted": False},
        {"customer_id": "CHAIN-002", "display_name": "Bob", "segment": "SILVER", "is_deleted": False},
    ],
    [
        {"customer_id": "CHAIN-001", "display_name": "Alicia", "segment": "SILVER", "is_deleted": False},
        {"customer_id": "CHAIN-002", "display_name": "Bob", "segment": "SILVER", "is_deleted": False},
    ],
    [
        {"customer_id": "CHAIN-001", "display_name": "Alicia", "segment": "SILVER", "is_deleted": True},
        {"customer_id": "CHAIN-002", "display_name": "Bob", "segment": "SILVER", "is_deleted": False},
    ],
]
if fingerprint(FIXTURES) != "00d63936aacd2cbdcd5cd535036a31f98b3c132111e35ebef8097434e51d3ff9":
    raise RuntimeError("Chain fixture differs from its immutable pin")
if framework_version != expected_framework_version or phase5_version != expected_phase5_version:
    raise RuntimeError("Chain proof requires its pinned published runtime")
spark.conf.set("spark.sql.session.timeZone", "UTC")
environment = Environment.from_dict(json.loads(environment_json))
control = ControlStore(spark, environment)
owner = control.claim(f"phase5-chain-probe:{uuid4()}", now=datetime.now(timezone.utc))
key_hash = F.udf(hash_key, "string")
payload_schema = spark_schema([
    {"name": "customer_id", "data_type": "STRING", "nullable": False},
    {"name": "display_name", "data_type": "STRING", "nullable": False},
    {"name": "segment", "data_type": "STRING", "nullable": False},
    {"name": "is_deleted", "data_type": "BOOLEAN", "nullable": False},
])
rules = [
    Rule("customer_key", "customer_id IS NOT NULL AND length(trim(customer_id)) > 0",
         "ERROR", "INVALID_CUSTOMER", 0, "FAIL_BATCH"),
    Rule("segment_key", "segment IS NOT NULL AND length(trim(segment)) > 0",
         "ERROR", "INVALID_SEGMENT", 0, "FAIL_BATCH"),
    Rule("delete_flag", "is_deleted IS NOT NULL", "ERROR", "MISSING_DELETE_FLAG", 0, "FAIL_BATCH"),
]


def stable(output):
    return {name: output[name] for name in ("target_table", "record_count", "output_fingerprint")}


def write(frame, role, schema, table, key, batch):
    return write_immutable_output(
        control, owner, frame, role=role, schema=schema, table=table, key=key,
        scope={"project_id": environment.project_id, "batch_id": batch.batch_id},
    )


def raw_records(fixture, delivery, batch, sequence):
    raw_text = "".join(canonical_json(record).decode() + "\n" for record in fixture)
    digest = hashlib.sha256(raw_text.encode()).hexdigest()
    document = {
        "contract_version": "1.0", "project_id": environment.project_id,
        "source_system_id": SOURCE_SYSTEM_ID, "delivery_id": delivery,
        "delivery_sequence": sequence, "schema_contract_version": RELEASE_ID,
        "status": "CLOSED", "objects": [{
            "source_object_id": SOURCE_OBJECT_ID,
            "files": [{"path": "customers.json", "size_bytes": len(raw_text.encode()), "sha256": digest}],
            "complete_snapshot": False, "empty_snapshot": False,
        }],
    }
    manifest = Manifest.from_dict(
        document, project_id=environment.project_id, source_system_id=SOURCE_SYSTEM_ID,
        required_objects={SOURCE_OBJECT_ID}, schema_contract_version=RELEASE_ID,
    )
    root = (f"abfss://{environment.workspace_name}@onelake.dfs.fabric.microsoft.com/"
            f"{environment.lakehouses['processing'].name}.Lakehouse/Files/phase5-proof/chain-probe-v1/{delivery[-2:]}")
    files = {"customers.json": raw_text, "_manifest.json": canonical_json(document).decode()}
    notebookutils.fs.mkdirs(root)
    existing = notebookutils.fs.ls(root)
    if any(entry.isDir for entry in existing) or {entry.name for entry in existing} - set(files):
        raise RuntimeError("Chain Raw fixture folder has an unexpected inventory")
    names = {entry.name for entry in existing}
    for name, text in files.items():
        path = f"{root}/{name}"
        if name not in names and not notebookutils.fs.put(path, text, False):
            raise RuntimeError("Chain Raw file creation failed without overwrite")
        if notebookutils.fs.head(path, max_bytes=len(text.encode()) + 1) != text:
            raise RuntimeError("Chain Raw file content differs from its pinned fixture")
    entries = notebookutils.fs.ls(root)
    if any(entry.isDir for entry in entries) or {entry.name for entry in entries} != set(files):
        raise RuntimeError("Chain Raw inventory differs after readback")
    if any(entry.size != len(files[entry.name].encode()) for entry in entries):
        raise RuntimeError("Chain Raw file byte size differs")
    validate_file_inventory(manifest, [entry.name for entry in entries])
    return manifest, read_manifest_records(
        spark, root_path=root, manifest=manifest, project_id=environment.project_id,
        source_formats={SOURCE_OBJECT_ID: "JSON"}, schemas={SOURCE_OBJECT_ID: payload_schema},
        batch_id=batch.batch_id, release_id=RELEASE_ID, load_date_utc=batch.load_date_utc,
    )


history = None
seen_customers = None
seen_segments = None
seen_links = None
receipts = []
cached = []
try:
    control.assert_owner(owner)
    namespace = f"`{environment.workspace_name}`.`{environment.lakehouses['gold'].name}`.`current`"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {namespace}").collect()
    for index, fixture in enumerate(FIXTURES, 1):
        delivery = f"PHASE5_CHAIN_PROBE|{index:02d}"
        now = datetime.now(timezone.utc)
        candidate = BatchContext.create(
            environment.project_id, SOURCE_SYSTEM_ID, delivery, RELEASE_ID, load_date=now,
        )
        existing = rows(spark, f"SELECT delivery_sequence FROM {control.table('batch_context')} "
                        f"WHERE batch_id = {literal(candidate.batch_id)}")
        if len(existing) > 1:
            raise RuntimeError("Chain batch context is duplicated")
        if existing:
            sequence = existing[0]["delivery_sequence"]
        else:
            previous = rows(spark, f"SELECT MAX(delivery_sequence) AS sequence "
                            f"FROM {control.table('batch_context')} "
                            f"WHERE project_id = {literal(environment.project_id)} "
                            f"AND source_system_id = {literal(SOURCE_SYSTEM_ID)}")
            sequence = (previous[0]["sequence"] or 0) + 1
        batch = control.batch_context(
            owner, SOURCE_SYSTEM_ID, delivery, sequence, RELEASE_ID, now=now, proof_only=True,
        )
        manifest, records = raw_records(fixture, delivery, batch, sequence)
        records = records.cache()
        cached.append(records)
        bronze = write_bronze_delivery(
            spark, records, target_table=environment.spark_table("processing", "bronze", "phase5_chain_customers"),
            project_id=environment.project_id, source_system_id=SOURCE_SYSTEM_ID,
            delivery_id=delivery, control=control, owner=owner,
        )
        bronze_commit = record_bronze_commit(
            control, owner, batch_id=batch.batch_id, source_system_id=SOURCE_SYSTEM_ID,
            delivery_id=delivery, manifest=manifest, outputs=stable(bronze), now=now,
        )
        outcomes, accounting = evaluate_quality(records, payload_schema=payload_schema, rules=rules)
        outcomes = outcomes.cache()
        cached.append(outcomes)
        if accounting["counts"] != {"FILTERED": 0, "PASSED": len(fixture), "REJECTED": 0}:
            raise RuntimeError("Chain delivery did not pass exact Quality accounting")
        quality = write(outcomes, "processing", "quality", "phase5_chain_customers", "raw_record_id", batch)
        quality_commit = commit_layer(
            control, owner, batch_id=batch.batch_id, layer="QUALITY", entity=SOURCE_OBJECT_ID,
            input_fingerprint=fingerprint([bronze_commit["commit_id"], RELEASE_ID]),
            outputs={"target": stable(quality), "accounting": accounting}, now=now,
        )
        candidates = outcomes.select(
            "project_id", "batch_id", "delivery_id", "delivery_sequence", "load_date_utc",
            F.from_json("typed_payload_json", payload_schema).alias("customer"),
        ).select(
            "project_id", "batch_id", "delivery_id", "delivery_sequence", "load_date_utc", "customer.*",
        ).distinct()
        if candidates.groupBy("customer_id").count().where(F.col("count") != 1).limit(1).count():
            raise RuntimeError("Conflicting payloads for a businesskey in one delivery")
        candidates = (
            candidates.withColumn("hk_customer", key_hash(F.lit("CHAIN_CUSTOMER"), F.array("customer_id")))
            .withColumn("hk_segment", key_hash(F.lit("CHAIN_SEGMENT"), F.array("segment")))
            .withColumn("hk_link", key_hash(F.lit("CHAIN_CUSTOMER_SEGMENT"), F.array("customer_id", "segment")))
            .withColumn("hashdiff", key_hash(F.lit("CHAIN_CUSTOMER_PAYLOAD"), F.array(
                F.col("display_name"), F.col("segment"), F.col("is_deleted").cast("string"),
            )))
        ).cache()
        cached.append(candidates)
        customer_hubs = candidates.select(
            "project_id", "batch_id", "hk_customer", "customer_id", "load_date_utc",
        )
        segment_hubs = candidates.select(
            "project_id", "batch_id", "hk_segment", "segment", "load_date_utc",
        ).distinct()
        links = candidates.select(
            "project_id", "batch_id", "hk_link", "hk_customer", "hk_segment", "load_date_utc",
        )
        if seen_customers is not None:
            customer_hubs = customer_hubs.join(seen_customers, "hk_customer", "left_anti").select(
                "project_id", "batch_id", "hk_customer", "customer_id", "load_date_utc",
            )
            segment_hubs = segment_hubs.join(seen_segments, "hk_segment", "left_anti").select(
                "project_id", "batch_id", "hk_segment", "segment", "load_date_utc",
            )
            links = links.join(seen_links, "hk_link", "left_anti").select(
                "project_id", "batch_id", "hk_link", "hk_customer", "hk_segment", "load_date_utc",
            )
        events = candidates
        if history is not None:
            latest = history.withColumn(
                "_rank", F.row_number().over(Window.partitionBy("hk_customer").orderBy(F.desc("delivery_sequence"))),
            ).where(F.col("_rank") == 1).select("hk_customer", F.col("hashdiff").alias("_previous_hash"))
            events = candidates.join(latest, "hk_customer", "left").where(
                ~F.col("hashdiff").eqNullSafe(F.col("_previous_hash")),
            ).select(*candidates.columns)
        events = events.withColumn(
            "event_id", key_hash(F.lit("CHAIN_CUSTOMER_EVENT"), F.array("delivery_id", "hk_customer")),
        ).select(
            "project_id", "batch_id", "delivery_id", "delivery_sequence", "load_date_utc",
            "event_id", "hk_customer", "customer_id", "display_name", "segment", "is_deleted", "hashdiff",
        ).cache()
        cached.append(events)
        if events.count() != (2 if index == 1 else 1):
            raise RuntimeError("Satellite must keep initial versions and only changed subsequent versions")
        raw_outputs = {
            "customer_hub": write(customer_hubs, "processing", "raw_vault", "phase5_chain_hub_customer", "hk_customer", batch),
            "segment_hub": write(segment_hubs, "processing", "raw_vault", "phase5_chain_hub_segment", "hk_segment", batch),
            "link": write(links, "processing", "raw_vault", "phase5_chain_link_customer_segment", "hk_link", batch),
            "satellite": write(events, "processing", "raw_vault", "phase5_chain_sat_customer", "event_id", batch),
        }
        vault_commit = commit_layer(
            control, owner, batch_id=batch.batch_id, layer="RAW_VAULT", entity=SOURCE_OBJECT_ID,
            input_fingerprint=fingerprint([quality_commit["commit_id"], RELEASE_ID]),
            outputs={name: stable(output) for name, output in raw_outputs.items()}, now=now,
        )
        seen_customers = candidates.select("hk_customer").distinct() if seen_customers is None else (
            seen_customers.unionByName(candidates.select("hk_customer")).distinct()
        )
        seen_segments = candidates.select("hk_segment").distinct() if seen_segments is None else (
            seen_segments.unionByName(candidates.select("hk_segment")).distinct()
        )
        seen_links = candidates.select("hk_link").distinct() if seen_links is None else (
            seen_links.unionByName(candidates.select("hk_link")).distinct()
        )
        history = events if history is None else history.unionByName(events)
        history = history.cache()
        cached.append(history)
        bv = events.select(
            "project_id", "batch_id", "event_id", "hk_customer",
            F.upper("display_name").alias("display_name_upper"),
            F.when(F.col("is_deleted"), F.lit(False)).otherwise(F.lit(True)).alias("is_contactable"),
        )
        latest_history = history.withColumn(
            "_rank", F.row_number().over(Window.partitionBy("hk_customer").orderBy(F.desc("delivery_sequence"))),
        ).where(F.col("_rank") == 1)
        pit = latest_history.select(
            "project_id", F.lit(batch.batch_id).alias("batch_id"), "hk_customer",
            F.col("event_id").alias("satellite_event_id"),
            F.lit(batch.load_date_utc).alias("snapshot_time"),
        ).withColumn("pit_id", key_hash(F.lit("CHAIN_PIT"), F.array("batch_id", "hk_customer")))
        bv_outputs = {
            "satellite": write(bv, "processing", "business_vault", "phase5_chain_sat_customer_bv", "event_id", batch),
            "pit": write(pit, "processing", "business_vault", "phase5_chain_pit_customer", "pit_id", batch),
        }
        bv_commit = commit_layer(
            control, owner, batch_id=batch.batch_id, layer="BUSINESS_VAULT", entity=SOURCE_OBJECT_ID,
            input_fingerprint=fingerprint([vault_commit["commit_id"], RELEASE_ID]),
            outputs={name: stable(output) for name, output in bv_outputs.items()}, now=now,
        )
        product_release_id = fingerprint([vault_commit["commit_id"], bv_commit["commit_id"], "CHAIN_CUSTOMERS"])
        intervals = history.withColumn(
            "valid_to", F.lead("load_date_utc").over(
                Window.partitionBy("hk_customer").orderBy("delivery_sequence"),
            ),
        ).withColumn("is_current", F.col("valid_to").isNull())
        gold_history = intervals.select(
            "project_id", F.lit(batch.batch_id).alias("batch_id"),
            F.lit(product_release_id).alias("product_release_id"),
            "event_id", "hk_customer", "customer_id", "display_name", "segment",
            "is_deleted", F.col("load_date_utc").alias("valid_from"), "valid_to", "is_current",
        ).withColumn("gold_id", key_hash(F.lit("CHAIN_GOLD"), F.array("product_release_id", "event_id")))
        gold_current = gold_history.where(F.col("is_current") & ~F.col("is_deleted"))
        expected_history = (2, 3, 4)[index - 1]
        expected_current = (2, 2, 1)[index - 1]
        if gold_history.count() != expected_history or gold_current.count() != expected_current:
            raise RuntimeError("Gold historical/current counts differ from change/delete semantics")
        if gold_history.where(
            F.col("valid_to").isNotNull() & (F.col("valid_to") <= F.col("valid_from")),
        ).count():
            raise RuntimeError("Gold historical intervals are empty or reversed")
        gold_outputs = {
            "historical": write(gold_history, "gold", "historical", "phase5_chain_customer_releases", "gold_id", batch),
            "current": write(gold_current, "gold", "current", "phase5_chain_customer_releases", "gold_id", batch),
        }
        gold_commit = commit_layer(
            control, owner, batch_id=batch.batch_id, layer="GOLD", entity="CHAIN_CUSTOMERS",
            input_fingerprint=fingerprint([vault_commit["commit_id"], bv_commit["commit_id"], RELEASE_ID]),
            outputs={
                "product_release_id": product_release_id,
                "targets": {name: stable(output) for name, output in gold_outputs.items()},
            }, now=now,
        )
        receipts.append({
            "delivery_id": delivery, "batch_id": batch.batch_id, "load_date_utc": batch.load_date_utc,
            "bronze": bronze, "quality": quality, "raw_vault": raw_outputs,
            "business_vault": bv_outputs, "gold": gold_outputs, "accounting": accounting,
            "commits": {"bronze": bronze_commit, "quality": quality_commit, "vault": vault_commit,
                        "business_vault": bv_commit, "gold": gold_commit},
        })
finally:
    for frame in cached:
        frame.unpersist()
    control.release(owner)

notebookutils.notebook.exit(json.dumps({
    "status": "SUCCEEDED", "scope": "PHASE5_SYNTHETIC_CHAIN_THREE_DELIVERIES",
    "phase5_version": phase5_version, "deliveries": receipts, "owner_released": True,
    "production_release_accepted": False,
}, sort_keys=True))
