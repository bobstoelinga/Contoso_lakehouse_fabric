"""Quality with exact committed Bronze and same-delivery reference readback."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from fabric_etl.contracts import ContractError, fingerprint, identifier
from fabric_etl.control import literal, rows
from fabric_etl.project import ProjectRelease
from fabric_etl_phase5.ingestion import _output_fingerprint, spark_schema
from fabric_etl_phase5.mapping import evaluate_metadata_quality, quality_spec
from fabric_etl_phase5.quality import commit_layer, frame_fingerprint, write_immutable_output
from fabric_etl_phase6 import __version__


def required_references(project: ProjectRelease, source_id: str) -> list[str]:
    bindings = {item["id"]: "reference" for item in project.document["source_objects"]}
    return quality_spec(project, source_id, reference_views=bindings)["reference_sources"]


def quality_order(project: ProjectRelease, source_ids: set[str]) -> list[str]:
    if not source_ids:
        raise ContractError("Quality requires a nonempty source scope")
    dependencies = {source: set(required_references(project, source)) for source in source_ids}
    if any(not parents.issubset(source_ids) for parents in dependencies.values()):
        raise ContractError("Quality scope omits a required reference source")
    ordered: list[str] = []
    while len(ordered) < len(source_ids):
        ready = sorted(source for source, parents in dependencies.items()
                       if source not in ordered and parents.issubset(ordered))
        if not ready:
            raise ContractError("Quality references contain a dependency cycle")
        ordered.extend(ready)
    return ordered


def committed_output(
    control: Any, *, commit_id: str, batch_id: str, layer: str, entity: str,
    source_id: str, target: str, role: str, schema: str, table: str,
    source_system_id: str, delivery_id: str, release_id: str,
) -> tuple[Any, dict[str, Any]]:
    """Resolve only a declared target from the exact authoritative commit."""
    from pyspark.sql import functions as F

    if target != control.environment.spark_table(role, schema, identifier(table)):
        raise ContractError("Committed output target differs from the explicit binding")
    stored = rows(control.spark, f"SELECT * FROM {control.table('commit_manifest')} "
                  f"WHERE commit_id = {literal(commit_id)}")
    if len(stored) != 1 or any(stored[0].get(key) != value for key, value in {
        "batch_id": batch_id, "layer": layer, "entity_id": entity, "status": "COMMITTED",
    }.items()):
        raise ContractError("Committed output has no matching authoritative manifest")
    document = json.loads(stored[0]["outputs_json"])
    if layer == "INGEST":
        output = document["targets"][source_id]
    elif layer == "QUALITY":
        output = document["targets"]["PASSED"]
    else:
        raise ContractError("Unregistered Quality input layer")
    if output["target_table"] != target:
        raise ContractError("Commit target differs from the requested output")
    frame = control.spark.table(target).where(
        (F.col("project_id") == control.project) & (F.col("batch_id") == batch_id),
    )
    for key, expected in {
        "source_system_id": source_system_id, "delivery_id": delivery_id,
        "release_id": release_id, "source_object_id": source_id,
    }.items():
        if frame.where(F.col(key).isNull() | (F.col(key) != expected)).limit(1).count():
            raise ContractError("Committed output contains foreign delivery/release/source context")
    if layer == "QUALITY":
        if frame.where(F.col("terminal_status") != "PASSED").limit(1).count():
            raise ContractError("Committed reference contains non-PASSED records")
        count, digest = frame_fingerprint(frame, "raw_record_id")
    else:
        count, digest = _output_fingerprint(frame)
    if count != output["record_count"] or digest != output["output_fingerprint"]:
        raise ContractError("Committed output bytes/count differ from its manifest")
    return frame, output


def write_quality(
    control: Any, owner: Any, *, project: ProjectRelease, source_id: str,
    source_schema: Any, ingest: dict[str, Any], bronze_table: str,
    reference_commits: dict[str, dict[str, Any]], now: datetime,
    table: str, proof_only: bool = False,
) -> dict[str, Any]:
    from pyspark.sql import functions as F

    identifier(table)
    if proof_only is not True or not table.startswith("phase6_"):
        raise ContractError("Phase-six candidate writes require explicit isolated proof scope")
    if control.project != project.document["project_id"]:
        raise ContractError("Quality project differs from the control binding")
    needed = required_references(project, source_id)
    if set(reference_commits) != set(needed):
        raise ContractError("Quality needs the exact committed reference inventory")
    batch_id = ingest["batch_id"]
    batches = rows(control.spark, f"SELECT * FROM {control.table('batch_context')} "
                   f"WHERE batch_id = {literal(batch_id)}")
    if len(batches) != 1 or any(batches[0].get(key) != value for key, value in {
        "project_id": control.project, "source_system_id": "PHASE4_PROOF",
        "release_id": project.release_id,
    }.items()):
        raise ContractError("Quality requires the persisted proof batch of this candidate")
    batch = batches[0]
    control.assert_owner(owner)
    records, bronze = committed_output(
        control, commit_id=ingest["commit"]["commit_id"], batch_id=batch_id,
        layer="INGEST", entity="PHASE4_PROOF." + batch["delivery_id"],
        source_id=source_id, target=ingest["outputs"][source_id]["target_table"],
        role="processing", schema="bronze", table=bronze_table,
        source_system_id="PHASE4_PROOF", delivery_id=batch["delivery_id"], release_id=project.release_id,
    )
    if records.limit(1).count() == 0:
        raise ContractError("Phase-six Quality empty input needs an explicit typed-empty context extension")
    references = {}
    for reference_id, receipt in reference_commits.items():
        frame, _ = committed_output(
            control, commit_id=receipt["commit"]["commit_id"], batch_id=batch_id,
            layer="QUALITY", entity=reference_id, source_id=reference_id,
            target=receipt["outputs"]["PASSED"]["target_table"],
            role="processing", schema="quality", table=receipt["table"],
            source_system_id="PHASE4_PROOF", delivery_id=batch["delivery_id"], release_id=project.release_id,
        )
        reference_schema = next(item for item in project.document["schemas"] if item["id"] == next(
            source["schema_id"] for source in project.document["source_objects"] if source["id"] == reference_id
        ))
        references[reference_id] = frame.withColumn(
            "_typed", F.from_json("typed_payload_json", spark_schema(reference_schema["columns"])),
        ).select(*frame.columns, "_typed.*")
    outcomes, accounting = evaluate_metadata_quality(
        records, source_schema=source_schema, project=project,
        source_id=source_id, reference_frames=references,
    )
    outputs = {}
    for status, schema, suffix in (
        ("PASSED", "quality", ""), ("REJECTED", "reject", ""), ("FILTERED", "quality", "_filtered"),
    ):
        outputs[status] = write_immutable_output(
            control, owner, outcomes.where(F.col("terminal_status") == status),
            role="processing", schema=schema, table=table + suffix, key="raw_record_id",
            scope={"project_id": control.project, "batch_id": batch_id},
        )
    stable = {key: {name: output[name] for name in (
        "target_table", "record_count", "output_fingerprint",
    )} for key, output in outputs.items()}
    digest = fingerprint({
        "release_id": project.release_id, "runtime": __version__, "bronze": bronze,
        "ingest_commit": ingest["commit"]["commit_id"],
        "references": {key: value["commit"]["commit_id"] for key, value in reference_commits.items()},
    })
    commit = commit_layer(
        control, owner, batch_id=batch_id, layer="QUALITY", entity=source_id,
        input_fingerprint=digest, outputs={"targets": stable, "accounting": accounting}, now=now,
    )
    return {
        "source_id": source_id, "table": table, "batch_id": batch_id,
        "accounting": accounting, "outputs": outputs, "commit": commit,
        "reference_commit_ids": {key: value["commit"]["commit_id"] for key, value in reference_commits.items()},
    }
