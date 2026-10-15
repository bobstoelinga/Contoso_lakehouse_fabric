"""Durable, owner-gated closed deliveries with byte checks and recoverable work."""

from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime
from typing import Any, Callable

from fabric_etl.contracts import ContractError, canonical_json, fingerprint, identifier
from fabric_etl.control import literal, rows, timestamp
from fabric_etl.delivery import Manifest, RegisteredDelivery, next_delivery
from fabric_etl_phase5.ingestion import read_manifest_records, write_bronze_delivery
from fabric_etl_phase5.quality import commit_layer


class InterruptedDelivery(RuntimeError):
    """Explicit proof interruption; leave durable state for a separate-job recovery."""


def _file_identity(spark: Any, path: str) -> dict[str, Any]:
    jvm = spark._jvm
    hadoop_path = jvm.org.apache.hadoop.fs.Path(path)
    filesystem = hadoop_path.getFileSystem(spark._jsc.hadoopConfiguration())
    digest = jvm.java.security.MessageDigest.getInstance("SHA-256")
    stream = jvm.java.security.DigestInputStream(filesystem.open(hadoop_path), digest)
    sink = jvm.java.io.OutputStream.nullOutputStream()
    try:
        size = stream.transferTo(sink)
    finally:
        try:
            stream.close()
        finally:
            sink.close()
    return {"length": size, "sha256": bytes(value & 0xFF for value in digest.digest()).hex()}


def verify_onelake_files(spark: Any, fs: Any, root: str, manifest: Manifest) -> list[dict[str, Any]]:
    actual: set[str] = set()
    manifest_seen = False

    def walk(path: str, prefix: str) -> None:
        nonlocal manifest_seen
        for entry in fs.ls(path):
            relative = prefix + entry.name
            if entry.isDir:
                walk(entry.path, relative.rstrip("/") + "/")
            elif relative == "_manifest.json":
                manifest_seen = True
                if entry.size > 1024 * 1024:
                    raise ContractError("Delivery manifest exceeds the one-MiB control contract")
                if json.loads(fs.head(entry.path, max_bytes=entry.size + 1)) != _document(manifest):
                    raise ContractError("OneLake manifest differs from the registered document")
            else:
                actual.add(relative)

    walk(root, "")
    expected = {file.path for obj in manifest.objects for file in obj.files}
    if not manifest_seen or actual != expected:
        raise ContractError("OneLake delivery inventory differs from the closed manifest")
    observed = []
    for obj in manifest.objects:
        for file in obj.files:
            result = _file_identity(spark, root.rstrip("/") + "/" + file.path)
            if result["length"] != file.size_bytes or result["sha256"] != file.sha256:
                raise ContractError(f"OneLake file bytes differ from manifest: {file.path}")
            observed.append({"path": file.path, "size_bytes": file.size_bytes, "sha256": file.sha256})
    return sorted(observed, key=lambda value: value["path"])


def register_delivery(control: Any, owner: Any, document: dict, manifest: Manifest, *, now: datetime) -> dict:
    """scope_json persists the full immutable manifest, including every file identity."""
    control.assert_owner(owner)
    if manifest.project_id != control.project:
        raise ContractError("Delivery registration belongs to another project")
    table = control.table("delivery")
    scope = canonical_json(document).decode()
    digest = fingerprint(document)
    predicate = (
        f"project_id = {literal(control.project)} AND source_system_id = {literal(manifest.source_system_id)}"
    )
    existing = rows(control.spark, f"SELECT * FROM {table} WHERE {predicate}")
    matching = [row for row in existing if row["delivery_id"] == manifest.delivery_id]
    if matching:
        if (
            len(matching) != 1 or matching[0]["delivery_sequence"] != manifest.delivery_sequence
            or matching[0]["manifest_fingerprint"] != digest or matching[0]["scope_json"] != scope
        ):
            raise ContractError("Registered delivery manifest/sequence conflicts with immutable input")
        return matching[0]
    if any(row["delivery_sequence"] >= manifest.delivery_sequence for row in existing):
        raise ContractError("New delivery registration is not chronologically increasing")
    control.spark.sql(
        f"INSERT INTO {table} SELECT {literal(control.project)}, {literal(manifest.source_system_id)}, "
        f"{literal(manifest.delivery_id)}, {manifest.delivery_sequence}, {literal(digest)}, "
        f"{literal(scope)}, 'REGISTERED', NULL, {timestamp(now)}"
    ).collect()
    control.assert_owner(owner)
    observed = rows(control.spark, f"SELECT * FROM {table} WHERE {predicate} "
                    f"AND delivery_id = {literal(manifest.delivery_id)}")
    if len(observed) != 1 or observed[0]["scope_json"] != scope or observed[0]["manifest_fingerprint"] != digest:
        raise ContractError("Delivery registration readback differs")
    return observed[0]


def _delivery_status(control: Any, owner: Any, manifest: Manifest, status: str) -> None:
    if status not in {"READY", "PROCESSING", "SUCCEEDED", "FAILED"}:
        raise ContractError("Unsupported delivery transition")
    control.assert_owner(owner)
    predicate = (
        f"project_id = {literal(control.project)} AND source_system_id = {literal(manifest.source_system_id)} "
        f"AND delivery_id = {literal(manifest.delivery_id)} "
        f"AND manifest_fingerprint = {literal(fingerprint(_document(manifest)))}"
    )
    control.spark.sql(f"UPDATE {control.table('delivery')} SET status = {literal(status)} WHERE {predicate}").collect()
    observed = rows(control.spark, f"SELECT * FROM {control.table('delivery')} WHERE {predicate}")
    if len(observed) != 1 or observed[0]["status"] != status:
        raise ContractError("Delivery transition readback failed")


def _document(manifest: Manifest) -> dict:
    value = asdict(manifest)
    value.update(contract_version="1.0", status="CLOSED")
    # dataclass tuples canonicalize to JSON lists.
    return json.loads(canonical_json(value))


def _audit(control: Any, owner: Any, batch_id: str, entity: str, attempt: int,
           transition: str, error: str | None, *, now: datetime) -> str:
    control.assert_owner(owner)
    event_id = fingerprint([control.project, batch_id, "INGEST", entity, attempt, transition])
    table = control.environment.spark_table("control", "audit", "events")
    predicate = f"event_id = {literal(event_id)}"
    content = canonical_json({"entity": entity, "attempt": attempt, "transition": transition, "error": error}).decode()
    existing = rows(control.spark, f"SELECT * FROM {table} WHERE {predicate}")
    if existing and (len(existing) != 1 or existing[0]["context_json"] != content):
        raise ContractError("Ingestion audit event conflict")
    if not existing:
        control.spark.sql(
            f"INSERT INTO {table} SELECT {literal(event_id)}, {literal(control.project)}, "
            f"{literal(batch_id)}, {literal(batch_id)}, 'INGEST', {literal(entity)}, {attempt}, "
            f"{literal(transition)}, {timestamp(now)}, "
            f"{literal(error) if error else 'NULL'}, {literal(content)}"
        ).collect()
    observed = rows(control.spark, f"SELECT * FROM {table} WHERE {predicate}")
    if len(observed) != 1 or observed[0]["context_json"] != content:
        raise ContractError("Ingestion audit readback failed")
    return event_id


def _work(control: Any, owner: Any, batch_id: str, entity: str, digest: str,
          attempt: int, status: str, commit: str | None, error: str | None, *, now: datetime) -> None:
    control.assert_owner(owner)
    table = control.table("work_item")
    predicate = f"batch_id = {literal(batch_id)} AND layer = 'INGEST' AND entity_id = {literal(entity)}"
    existing = rows(control.spark, f"SELECT * FROM {table} WHERE {predicate}")
    if existing and (len(existing) != 1 or existing[0]["input_fingerprint"] != digest):
        raise ContractError("Ingestion work input/cardinality conflict")
    commit_sql, error_sql = literal(commit) if commit else "NULL", literal(error) if error else "NULL"
    if existing:
        control.spark.sql(
            f"UPDATE {table} SET status = {literal(status)}, commit_id = {commit_sql}, attempt = {attempt}, "
            f"error_code = {error_sql}, updated_at = {timestamp(now)} WHERE {predicate}"
        ).collect()
    else:
        control.spark.sql(
            f"INSERT INTO {table} SELECT {literal(batch_id)}, 'INGEST', {literal(entity)}, "
            f"{literal(digest)}, {literal(status)}, {commit_sql}, {attempt}, {error_sql}, {timestamp(now)}"
        ).collect()
    observed = rows(control.spark, f"SELECT * FROM {table} WHERE {predicate}")
    if (
        len(observed) != 1 or observed[0]["status"] != status or observed[0]["attempt"] != attempt
        or observed[0]["commit_id"] != commit or observed[0]["error_code"] != error
    ):
        raise ContractError("Ingestion work readback failed")


def ingest_registered_delivery(
    control: Any, owner: Any, *, fs: Any, root: str, document: dict,
    required_objects: set[str], source_formats: dict[str, str], schemas: dict[str, Any],
    targets: dict[str, str], now: datetime, proof_only: bool = False,
    options: dict[str, dict[str, str | bool]] | None = None,
    boundary: Callable[[str], None] | None = None,
) -> dict:
    """Persist gate/work/audit, reconcile all outputs, then release downstream by commit."""
    from pyspark.sql import functions as F
    from fabric_etl_phase5 import __version__

    manifest = Manifest.from_dict(
        document, project_id=control.project, source_system_id=document["source_system_id"],
        required_objects=required_objects, schema_contract_version=document["schema_contract_version"],
    )
    if document != _document(manifest):
        raise ContractError("Delivery document differs from its canonical closed contract")
    if set(targets) != required_objects or set(source_formats) != required_objects or set(schemas) != required_objects:
        raise ContractError("Ingestion runtime bindings must cover the exact required object scope")
    for name in targets.values():
        identifier(name)
        if proof_only and not name.startswith("phase5_exit_"):
            raise ContractError("Exit proof target is not isolated")
    if boundary is not None and proof_only is not True:
        raise ContractError("Injected boundaries are restricted to explicit proofs")
    if proof_only and manifest.source_system_id != "PHASE4_PROOF":
        raise ContractError("Proof delivery source must be PHASE4_PROOF")
    row = register_delivery(control, owner, document, manifest, now=now)
    if boundary:
        boundary("registered")
    files = verify_onelake_files(control.spark, fs, root, manifest)
    _delivery_status(control, owner, manifest, "READY" if row["status"] != "SUCCEEDED" else "SUCCEEDED")
    registered = rows(
        control.spark, f"SELECT * FROM {control.table('delivery')} "
        f"WHERE project_id = {literal(control.project)} AND source_system_id = {literal(manifest.source_system_id)}",
    )
    selected = next_delivery([RegisteredDelivery(r["delivery_id"], r["delivery_sequence"], r["status"]) for r in registered])
    if row["status"] != "SUCCEEDED" and (selected is None or selected.delivery_id != manifest.delivery_id):
        raise ContractError("An older unfinished registered delivery blocks this delivery")
    batch = control.batch_context(
        owner, manifest.source_system_id, manifest.delivery_id, manifest.delivery_sequence,
        manifest.schema_contract_version, now=now, proof_only=proof_only,
    )
    entity = manifest.source_system_id + "." + manifest.delivery_id
    digest = fingerprint({
        "manifest": document, "root": root, "formats": source_formats, "targets": targets,
        "schemas": {key: schema.json() for key, schema in schemas.items()}, "runtime": __version__,
        "options": options,
    })
    work = rows(control.spark, f"SELECT * FROM {control.table('work_item')} "
                f"WHERE batch_id = {literal(batch.batch_id)} AND layer = 'INGEST' AND entity_id = {literal(entity)}")
    if work and (len(work) != 1 or work[0]["input_fingerprint"] != digest):
        raise ContractError("Retry input differs from the persisted work contract")
    # Preserve the successful attempt on replay so missing audit events can be repaired idempotently.
    attempt = work[0]["attempt"] if work and work[0]["status"] == "SUCCEEDED" else (work[0]["attempt"] + 1 if work else 1)
    _work(control, owner, batch.batch_id, entity, digest, attempt, "PROCESSING", None, None, now=now)
    _audit(control, owner, batch.batch_id, entity, attempt, "STARTED", None, now=now)
    _delivery_status(control, owner, manifest, "PROCESSING")
    records = None
    try:
        records = read_manifest_records(
            control.spark, root_path=root, manifest=manifest, project_id=control.project,
            source_formats=source_formats, schemas=schemas, batch_id=batch.batch_id,
            release_id=manifest.schema_contract_version, load_date_utc=batch.load_date_utc,
            options=options,
        ).cache()
        records.count()
        if boundary:
            boundary("raw")
        outputs = {}
        for source_id in sorted(required_objects):
            outputs[source_id] = write_bronze_delivery(
                control.spark, records.where(F.col("source_object_id") == source_id),
                target_table=control.environment.spark_table("processing", "bronze", targets[source_id]),
                project_id=control.project, source_system_id=manifest.source_system_id,
                delivery_id=manifest.delivery_id, control=control, owner=owner,
            )
        if boundary:
            boundary("bronze")
        verify_onelake_files(control.spark, fs, root, manifest)
        stable = {key: {name: output[name] for name in ("target_table", "record_count", "output_fingerprint")}
                  for key, output in outputs.items()}
        commit = commit_layer(
            control, owner, batch_id=batch.batch_id, layer="INGEST", entity=entity,
            input_fingerprint=digest, outputs={"targets": stable, "files": files, "manifest": document}, now=now,
        )
        if boundary:
            boundary("commit")
        _work(control, owner, batch.batch_id, entity, digest, attempt, "SUCCEEDED", commit["commit_id"], None, now=now)
        _delivery_status(control, owner, manifest, "SUCCEEDED")
        if boundary:
            boundary("status")
        audit_id = _audit(control, owner, batch.batch_id, entity, attempt, "SUCCEEDED", None, now=now)
        return {
            "batch_id": batch.batch_id, "load_date_utc": batch.load_date_utc,
            "outputs": outputs, "commit": commit, "audit_id": audit_id, "attempt": attempt,
            "files": files, "work_status": "SUCCEEDED", "delivery_status": "SUCCEEDED",
        }
    except InterruptedDelivery:
        raise
    except Exception as exc:
        # Persist explicit failure diagnostics and rethrow; never turn failed processing into a success result.
        error = type(exc).__name__
        _work(control, owner, batch.batch_id, entity, digest, attempt, "FAILED", None, error, now=now)
        _delivery_status(control, owner, manifest, "FAILED")
        _audit(control, owner, batch.batch_id, entity, attempt, "FAILED", error, now=now)
        raise
    finally:
        if records is not None:
            records.unpersist()
