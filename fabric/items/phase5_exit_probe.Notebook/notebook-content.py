# Fabric notebook source

# METADATA ********************
# META {
# META   "kernel_info": {"name": "synapse_pyspark"},
# META   "dependencies": {}
# META }

# PARAMETERS CELL ********************
environment_json = "{}"
expected_phase5_version = "0.6.1"

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
from pyspark.sql import functions as F, types as T

from fabric_etl.contracts import ContractError, Environment, canonical_json
from fabric_etl.control import ControlStore, literal, rows
from fabric_etl.delivery import Manifest
from fabric_etl_phase5 import __version__
from fabric_etl_phase5.delivery import (
    InterruptedDelivery, _file_identity, ingest_registered_delivery, register_delivery, verify_onelake_files,
)
from fabric_etl_phase5.ingestion import _output_fingerprint, read_manifest_records

if __version__ != expected_phase5_version:
    raise RuntimeError("Exit proof requires its exact published runtime")
environment = Environment.from_dict(json.loads(environment_json))
control = ControlStore(spark, environment)
fs = notebookutils.fs
base = (f"abfss://{environment.workspace_name}@onelake.dfs.fabric.microsoft.com/"
        f"{environment.lakehouses['processing'].name}.Lakehouse/Files/phase5-proof/exit-v1")
schema = T.StructType([T.StructField("key", T.StringType(), True), T.StructField("value", T.StringType(), True)])
payload = '{"key":"A","value":"1"}\n{"key":"A","value":"1"}\n{"key":"B","value":"2"}\n'
csv = 'key,value\nA,1\nA,1\nB,2\n'
release = "phase5-exit-v1"
now = datetime.now(timezone.utc)
owner = control.claim("phase5-exit:" + uuid4().hex, now=now)
reports, formats, negatives, interruptions = {}, {}, {}, {}
cross_pending = None


def put_exact(path, text):
    parent = path.rsplit("/", 1)[0]
    fs.mkdirs(parent)
    present = {entry.name for entry in fs.ls(parent)}
    name = path.rsplit("/", 1)[1]
    if name not in present and not fs.put(path, text, False):
        raise RuntimeError("Cannot create exit fixture without overwrite")
    if fs.head(path, max_bytes=len(text.encode()) + 1) != text:
        raise RuntimeError("Immutable exit fixture bytes differ")


def make_case(name, kinds, empty=False):
    root = base + "/" + name
    fs.mkdirs(root)
    delivery_id = "PHASE5_EXIT|" + name.upper()
    existing = rows(spark, f"SELECT * FROM {control.table('delivery')} "
                    f"WHERE project_id = {literal(control.project)} AND source_system_id = 'PHASE4_PROOF'")
    matching = [row for row in existing if row["delivery_id"] == delivery_id]
    previous = rows(spark, f"SELECT MAX(delivery_sequence) AS sequence FROM {control.table('batch_context')} "
                    f"WHERE project_id = {literal(control.project)} AND source_system_id = 'PHASE4_PROOF'")
    sequence = matching[0]["delivery_sequence"] if matching else max(
        [row["delivery_sequence"] for row in existing] + [previous[0]["sequence"] or 0],
    ) + 1
    objects, source_formats, schemas, targets, options = [], {}, {}, {}, {}
    for kind in kinds:
        source_id = "EXIT." + kind
        files = []
        if not empty:
            path = kind.lower() + "." + kind.lower()
            full = root + "/" + path
            if kind in {"JSON", "CSV"}:
                put_exact(full, payload if kind == "JSON" else csv)
            else:
                if path not in {entry.name for entry in fs.ls(root)}:
                    build = base + "_parquet_build"
                    if not fs.exists(build):
                        spark.createDataFrame([("A", "1"), ("A", "1"), ("B", "2")], schema).coalesce(1).write.parquet(build)
                    parts = [entry for entry in fs.ls(build) if entry.name.endswith(".parquet")]
                    if len(parts) != 1 or not fs.cp(parts[0].path, full):
                        raise RuntimeError("Cannot install immutable parquet fixture")
            binary = _file_identity(spark, full)
            files = [{"path": path, "size_bytes": binary["length"], "sha256": binary["sha256"]}]
        objects.append({
            "source_object_id": source_id, "files": files,
            "complete_snapshot": empty, "empty_snapshot": empty,
        })
        source_formats[source_id], schemas[source_id] = kind, schema
        targets[source_id] = "phase5_exit_" + name + "_" + kind.lower()
        if kind == "CSV":
            options[source_id] = {"header": True}
    document = dict(
        contract_version="1.0", project_id=control.project, source_system_id="PHASE4_PROOF",
        delivery_id=delivery_id, delivery_sequence=sequence, schema_contract_version=release,
        status="CLOSED", objects=objects,
    )
    put_exact(root + "/_manifest.json", canonical_json(document).decode())
    return dict(fs=fs, root=root, document=document, required_objects=set(source_formats),
                source_formats=source_formats, schemas=schemas, targets=targets, options=options,
                now=now, proof_only=True)


def expect_contract(name, action):
    try:
        action()
    except ContractError as exc:
        negatives[name] = str(exc)
    else:
        raise RuntimeError("Negative exit case did not block: " + name)


try:
    case = make_case("formats", ["JSON", "CSV", "PARQUET"])
    manifest = Manifest.from_dict(
        case["document"], project_id=control.project, source_system_id="PHASE4_PROOF",
        required_objects=case["required_objects"], schema_contract_version=release,
    )
    verify_onelake_files(spark, fs, case["root"], manifest)
    for kind in ("JSON", "CSV", "PARQUET"):
        source_id = "EXIT." + kind
        single = json.loads(canonical_json(case["document"]))
        single["objects"] = [obj for obj in single["objects"] if obj["source_object_id"] == source_id]
        one = Manifest.from_dict(single, project_id=control.project, source_system_id="PHASE4_PROOF",
                                 required_objects={source_id}, schema_contract_version=release)
        def read(declared, partitions):
            spark.conf.set("spark.sql.shuffle.partitions", str(partitions))
            return read_manifest_records(
                spark, root_path=case["root"], manifest=one, project_id=control.project,
                source_formats={source_id: kind}, schemas={source_id: declared},
                batch_id="format-identity-probe", release_id=release, load_date_utc="2026-10-11T00:00:00Z",
                options={source_id: {"header": True}} if kind == "CSV" else {},
            )
        first = sorted(tuple(row) for row in read(schema, 1).repartition(1).select(
            "raw_record_id", "payload_json", "duplicate_ordinal",
        ).collect())
        second = sorted(tuple(row) for row in read(schema, 7).repartition(7).select(
            "raw_record_id", "payload_json", "duplicate_ordinal",
        ).collect())
        if first != second or len(first) != 3 or sorted(row[2] for row in first) != [1, 1, 2]:
            raise RuntimeError("Format duplicate identity multiset changed: " + kind)
        formats[kind] = {
            "count": 3, "identity_multiset": first, "partitions": [1, 7], "exact_equal": True,
        }
        wrong = T.StructType([T.StructField("wrong", T.StringType(), True)])
        expect_contract("schema_" + kind, lambda: read(wrong, 2).count())
        if kind == "PARQUET":
            wrong_type = T.StructType([T.StructField("key", T.IntegerType(), True), T.StructField("value", T.StringType(), True)])
            expect_contract("parquet_type", lambda: read(wrong_type, 2).count())
    reports["formats"] = ingest_registered_delivery(control, owner, **case)
    changed = json.loads(canonical_json(case["document"]))
    changed["objects"][0]["files"][0]["sha256"] = "0" * 64
    bad = Manifest.from_dict(changed, project_id=control.project, source_system_id="PHASE4_PROOF",
                            required_objects=case["required_objects"], schema_contract_version=release)
    expect_contract("registered_manifest_conflict", lambda: register_delivery(control, owner, changed, bad, now=now))
    expect_contract("physical_manifest_conflict", lambda: verify_onelake_files(spark, fs, case["root"], bad))
    negative_root = base + "/negative_bytes"
    put_exact(negative_root + "/json.json", payload)
    byte_doc = json.loads(canonical_json(case["document"]))
    byte_doc["objects"] = [byte_doc["objects"][0]]
    byte_doc["objects"][0]["files"][0]["sha256"] = "0" * 64
    put_exact(negative_root + "/_manifest.json", canonical_json(byte_doc).decode())
    byte_manifest = Manifest.from_dict(
        byte_doc, project_id=control.project, source_system_id="PHASE4_PROOF",
        required_objects={"EXIT.JSON"}, schema_contract_version=release,
    )
    expect_contract("physical_file_bytes", lambda: verify_onelake_files(spark, fs, negative_root, byte_manifest))
    missing_root = base + "/negative_missing"
    put_exact(missing_root + "/_manifest.json", canonical_json(byte_doc).decode())
    expect_contract("physical_file_missing", lambda: verify_onelake_files(spark, fs, missing_root, byte_manifest))
    incomplete = json.loads(canonical_json(case["document"]))
    incomplete["objects"].pop()
    expect_contract("incomplete_object_scope", lambda: Manifest.from_dict(
        incomplete, project_id=control.project, source_system_id="PHASE4_PROOF",
        required_objects=case["required_objects"], schema_contract_version=release,
    ))
    # Persist boundary interruptions, recover without purge, and repeat the complete suite in a separate job.
    for name in ("registered", "raw", "bronze", "commit", "status"):
        case = make_case("stop_" + name, ["JSON"])
        previous = rows(spark, f"SELECT * FROM {control.table('delivery')} "
                        f"WHERE delivery_id = {literal(case['document']['delivery_id'])} AND project_id = {literal(control.project)}")
        inject = not previous or previous[0]["status"] != "SUCCEEDED"
        if inject:
            def stop(boundary_name):
                if boundary_name == name:
                    raise InterruptedDelivery(name)
            try:
                ingest_registered_delivery(control, owner, **case, boundary=stop)
            except InterruptedDelivery as exc:
                interruptions[name] = str(exc)
            else:
                raise RuntimeError("Requested interruption was not reached")
            pending = rows(spark, f"SELECT * FROM {control.table('delivery')} "
                           f"WHERE delivery_id = {literal(case['document']['delivery_id'])} AND project_id = {literal(control.project)}")
            if name != "status" and pending[0]["status"] == "SUCCEEDED":
                raise RuntimeError("Incomplete boundary released the delivery")
            if name == "registered":
                newer = make_case("newer_blocked", ["JSON"])
                expect_contract("older_delivery_blocks_newer", lambda: ingest_registered_delivery(control, owner, **newer))
        else:
            interruptions[name] = "PREVIOUS_INTERRUPTION_RECOVERED"
        reports[name] = ingest_registered_delivery(control, owner, **case)
        if name == "registered":
            newer = make_case("newer_blocked", ["JSON"])
            reports["newer"] = ingest_registered_delivery(control, owner, **newer)
    empty = make_case("empty", ["JSON"], empty=True)
    reports["empty"] = ingest_registered_delivery(control, owner, **empty)
    if reports["empty"]["outputs"]["EXIT.JSON"]["record_count"] != 0:
        raise RuntimeError("Explicit empty snapshot did not commit zero records")
    missing = json.loads(canonical_json(empty["document"]))
    missing["objects"][0]["empty_snapshot"] = False
    expect_contract("missing_is_not_empty", lambda: Manifest.from_dict(
        missing, project_id=control.project, source_system_id="PHASE4_PROOF",
        required_objects=empty["required_objects"], schema_contract_version=release,
    ))
    cross = make_case("cross_job", ["JSON"])
    previous = rows(spark, f"SELECT * FROM {control.table('delivery')} "
                    f"WHERE delivery_id = {literal(cross['document']['delivery_id'])} AND project_id = {literal(control.project)}")
    cross_job_interrupted = not previous
    if cross_job_interrupted:
        def cross_stop(name):
            if name == "bronze":
                raise InterruptedDelivery("cross-job after Bronze before commit")
        try:
            ingest_registered_delivery(control, owner, **cross, boundary=cross_stop)
        except InterruptedDelivery:
            pass
        else:
            raise RuntimeError("Cross-job boundary did not interrupt")
        pending = rows(spark, f"SELECT * FROM {control.table('delivery')} "
                       f"WHERE delivery_id = {literal(cross['document']['delivery_id'])} AND project_id = {literal(control.project)}")
        if len(pending) != 1 or pending[0]["status"] != "PROCESSING":
            raise RuntimeError("Cross-job incomplete delivery released downstream")
        batches = rows(spark, f"SELECT * FROM {control.table('batch_context')} "
                       f"WHERE delivery_id = {literal(cross['document']['delivery_id'])} "
                       f"AND project_id = {literal(control.project)} AND source_system_id = 'PHASE4_PROOF'")
        if len(batches) != 1:
            raise RuntimeError("Cross-job context missing")
        batch_id = batches[0]["batch_id"]
        commits = rows(spark, f"SELECT * FROM {control.table('commit_manifest')} "
                       f"WHERE batch_id = {literal(batch_id)} AND layer = 'INGEST'")
        if commits:
            raise RuntimeError("Interrupted Bronze was prematurely committed")
        target = environment.spark_table("processing", "bronze", cross["targets"]["EXIT.JSON"])
        count, digest = _output_fingerprint(spark.table(target).where(F.col("batch_id") == batch_id))
        if count != 3:
            raise RuntimeError("Cross-job interrupted Bronze is incomplete")
        cross_pending = {
            "batch_id": batch_id, "target_table": target, "record_count": count,
            "output_fingerprint": digest, "commit_count": 0, "delivery_status": "PROCESSING",
        }
    else:
        reports["cross_job"] = ingest_registered_delivery(control, owner, **cross)
finally:
    control.release(owner)
notebookutils.notebook.exit(json.dumps({
    "status": "SUCCEEDED", "scope": "PHASE5_EXIT_INGESTION_CONTROL",
    "phase5_version": __version__, "formats": formats, "negative_cases": negatives,
    "interruptions": interruptions, "deliveries": reports, "owner_released": True,
    "cross_job_interrupted": cross_job_interrupted,
    "cross_pending": cross_pending,
    "business_data_written": False, "production_release_accepted": False,
}, sort_keys=True))
