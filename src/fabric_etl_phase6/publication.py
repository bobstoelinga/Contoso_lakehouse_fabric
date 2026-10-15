"""Proof-only atomic Sales product pointer after complete committed-output readback."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from fabric_etl.contracts import ContractError, canonical_json
from fabric_etl.control import literal, rows, timestamp
from fabric_etl.project import ProjectRelease
from fabric_etl_phase5.quality import frame_fingerprint
from fabric_etl_phase6.gold import sales_gold_entities
from fabric_etl_phase6.vault import _proof_batch


PRODUCT_ID = "PHASE6_SALES_MART"


def validate_gold_commit(
    control: Any, *, project: ProjectRelease, receipt: dict[str, Any], batch_id: str,
) -> dict[str, Any]:
    from pyspark.sql import functions as F

    batch = _proof_batch(control, project, batch_id)
    records = rows(control.spark, f"SELECT * FROM {control.table('commit_manifest')} "
                   f"WHERE commit_id = {literal(receipt['commit']['commit_id'])}")
    if len(records) != 1 or any(records[0].get(key) != value for key, value in {
        "status": "COMMITTED", "layer": "GOLD", "entity_id": PRODUCT_ID, "batch_id": batch_id,
    }.items()):
        raise ContractError("Sales publication requires its exact authoritative Gold commit")
    outputs = json.loads(records[0]["outputs_json"])
    if outputs.get("product_release_id") != receipt["product_release_id"]:
        raise ContractError("Sales publication release identity differs from its Gold commit")
    entities = sales_gold_entities(project)
    if set(outputs.get("targets", {})) != {entity["id"] for entity in entities}:
        raise ContractError("Sales publication requires the complete eleven-entity Gold manifest")
    for entity in entities:
        output = outputs["targets"][entity["id"]]
        target = control.environment.spark_table(
            "gold", "historical" if entity["layer"] == "HISTORICAL" else "current",
            "phase6_" + entity["id"].lower(),
        )
        if output["target_table"] != target:
            raise ContractError("Sales publication Gold target differs from its proof binding")
        frame = control.spark.table(target).where(
            (F.col("project_id") == control.project) & (F.col("batch_id") == batch_id),
        )
        if frame.where(
            F.col("product_release_id").isNull() | (F.col("product_release_id") != receipt["product_release_id"]),
        ).limit(1).count():
            raise ContractError("Sales publication output has foreign product-release context")
        count, digest = frame_fingerprint(frame, "_gold_id")
        if count != output["record_count"] or digest != output["output_fingerprint"]:
            raise ContractError("Sales publication committed Gold output count/fingerprint changed")
    return {"manifest": outputs, "cutoff": batch["load_date"]}


def publish_gold(
    control: Any, owner: Any, *, project: ProjectRelease, receipt: dict[str, Any], batch_id: str,
    now: datetime, proof_only: bool = False,
) -> dict[str, Any]:
    if proof_only is not True:
        raise ContractError("Sales publication requires explicit isolated proof scope")
    control.assert_owner(owner)
    validated = validate_gold_commit(control, project=project, receipt=receipt, batch_id=batch_id)
    releases = control.environment.spark_table("gold", "publication", "product_release")
    active = control.environment.spark_table("gold", "publication", "active_product")
    release_id = receipt["product_release_id"]
    predicate = f"project_id = {literal(control.project)} AND product_id = {literal(PRODUCT_ID)}"
    inputs = canonical_json({"gold_commit_id": receipt["commit"]["commit_id"],
                             "input_commit_ids": receipt["input_commit_ids"]}).decode()
    manifest = canonical_json(validated["manifest"]).decode()
    cutoff = validated["cutoff"]
    cutoff_utc = cutoff.replace(tzinfo=timezone.utc) if cutoff.tzinfo is None else cutoff
    existing = rows(control.spark, f"SELECT * FROM {releases} WHERE {predicate} AND release_id = {literal(release_id)}")
    expected = {
        "input_commits_json": inputs, "entity_manifest_json": manifest, "cutoff": validated["cutoff"],
        "spark_validation": "VERIFIED", "sql_validation": "NOT_TESTED",
        "consumer_status": "PROOF_ONLY_NOT_PUBLISHED", "status": "COMMITTED",
    }
    if existing:
        if len(existing) != 1 or any(existing[0].get(key) != value for key, value in expected.items()):
            raise ContractError("Sales product release conflicts with its immutable manifest")
    else:
        control.assert_owner(owner)
        control.spark.sql(
            f"INSERT INTO {releases} SELECT {literal(control.project)}, {literal(PRODUCT_ID)}, "
            f"{literal(release_id)}, {literal(inputs)}, {literal(manifest)}, {timestamp(cutoff_utc)}, "
            f"'VERIFIED', 'NOT_TESTED', 'PROOF_ONLY_NOT_PUBLISHED', 'COMMITTED', {timestamp(now)}"
        ).collect()
        registered = rows(control.spark, f"SELECT * FROM {releases} WHERE {predicate} AND release_id = {literal(release_id)}")
        if len(registered) != 1 or any(registered[0].get(key) != value for key, value in expected.items()):
            raise ContractError("Sales product release readback differs from its immutable manifest")
    current = rows(control.spark, f"SELECT * FROM {active} WHERE {predicate}")
    if len(current) > 1:
        raise ContractError("Sales proof product pointer is duplicated")
    previous = None
    generation = 1
    if current:
        if current[0]["active_release_id"] == release_id:
            return {"status": "VERIFIED", "active_release_id": release_id, "generation": current[0]["generation"]}
        active_release = rows(control.spark, f"SELECT * FROM {releases} WHERE {predicate} "
                              f"AND release_id = {literal(current[0]['active_release_id'])}")
        if len(active_release) != 1 or active_release[0]["status"] != "COMMITTED":
            raise ContractError("Sales active pointer has no matching committed release")
        if active_release[0]["cutoff"] >= validated["cutoff"]:
            return {
                "status": "VERIFIED_SUPERSEDED", "active_release_id": current[0]["active_release_id"],
                "generation": current[0]["generation"],
            }
        previous = current[0]["active_release_id"]
        generation = current[0]["generation"] + 1
    control.assert_owner(owner)
    if current:
        control.spark.sql(
            f"UPDATE {active} SET active_release_id = {literal(release_id)}, "
            f"previous_release_id = {literal(previous)}, generation = {generation}, updated_at = {timestamp(now)} "
            f"WHERE {predicate} AND active_release_id = {literal(previous)} AND generation = {generation - 1}"
        ).collect()
    else:
        control.spark.sql(
            f"INSERT INTO {active} SELECT {literal(control.project)}, {literal(PRODUCT_ID)}, "
            f"{literal(release_id)}, CAST(NULL AS STRING), CAST(1 AS BIGINT), {timestamp(now)}"
        ).collect()
    control.assert_owner(owner)
    observed = rows(control.spark, f"SELECT * FROM {active} WHERE {predicate}")
    if len(observed) != 1 or observed[0]["active_release_id"] != release_id or observed[0]["generation"] != generation:
        raise ContractError("Sales proof product pointer readback differs")
    return {"status": "ACTIVATED", "active_release_id": release_id, "generation": generation}
