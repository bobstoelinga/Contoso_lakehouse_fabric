"""Committed as-of customer PIT snapshots with explicit satellite event pointers."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from fabric_etl.contracts import ContractError, fingerprint
from fabric_etl.project import ProjectRelease
from fabric_etl_phase5.quality import commit_layer, write_immutable_output
from fabric_etl_phase6.vault import _proof_batch, read_history, read_vault_commit, vault_spec


PIT_CONTRACT = "sales-customer-pit-v1-committed-event-pointers"
PIT_PARENTS = {
    "vault_entities:HUB_CUSTOMER", "vault_entities:SAT_CUSTOMER", "business_entities:SAT_CUSTOMER_BV",
}


def pit_spec(project: ProjectRelease) -> dict[str, Any]:
    entities = [entity for entity in project.document["business_entities"] if entity["id"] == "PIT_CUSTOMER"]
    if len(entities) != 1:
        raise ContractError("Customer PIT requires exactly one registered entity")
    entity = entities[0]
    if entity["type"] != "PIT" or entity["keys"] != ["hk_customer"] or set(entity["parents"]) != PIT_PARENTS:
        raise ContractError("Customer PIT identity or committed parent contract differs")
    if entity["temporal_policy"] != "COMMITTED_AS_OF":
        raise ContractError("Customer PIT requires the committed as-of temporal policy")
    return entity


def committed_state(
    control: Any, *, project: ProjectRelease, entity_id: str, receipt: dict[str, Any], batch_id: str,
) -> Any:
    business = entity_id in {entity["id"] for entity in project.document["business_entities"]}
    vault_spec(project, entity_id, business=business)
    if receipt["entity_id"] != entity_id:
        raise ContractError("Committed state receipt differs from the requested entity")
    table = "phase6_" + entity_id.lower()
    current = read_vault_commit(
        control, project=project, entity_id=entity_id, table=table, batch_id=batch_id,
        commit_id=receipt["commit"]["commit_id"], business=business,
    )
    prior = read_history(
        control, project=project, entity_id=entity_id, table=table, before_batch_id=batch_id,
        commit_ids=receipt["history_commit_ids"], empty=current.limit(0), business=business,
    )
    return prior.unionByName(current)


def project_pit(
    hub: Any, raw: Any, business: Any, *, project_id: str, batch_id: str, as_of: datetime,
) -> Any:
    from pyspark.sql import Window, functions as F

    if as_of.tzinfo is None or as_of.utcoffset() is None:
        raise ContractError("PIT as-of requires an explicit UTC timestamp")
    if as_of.utcoffset().total_seconds() != 0:
        raise ContractError("PIT as-of timestamp must be UTC")
    frames = {"hub": hub, "raw": raw, "business": business}
    for name, frame in frames.items():
        required = {"project_id", "hk_customer", "load_date", "_event_id"}
        if not required.issubset(frame.columns):
            raise ContractError("PIT parent lacks its explicit event identity: " + name)
        if frame.where(
            F.col("project_id").isNull() | (F.col("project_id") != project_id)
            | F.col("load_date").isNull() | F.col("hk_customer").isNull() | F.col("_event_id").isNull(),
        ).limit(1).count():
            raise ContractError("PIT parent has invalid project, key or event context")
        if frame.groupBy("hk_customer", "load_date").count().where(F.col("count") != 1).limit(1).count():
            raise ContractError("PIT parent event timestamps are ambiguous")
    hubs = hub.where(F.col("load_date") <= F.lit(as_of)).select("hk_customer")
    if hubs.groupBy("hk_customer").count().where(F.col("count") != 1).limit(1).count():
        raise ContractError("PIT hub identity is duplicated")
    result = hubs
    for label, frame in (("raw", raw), ("bv", business)):
        latest = frame.where(F.col("load_date") <= F.lit(as_of)).withColumn(
            "_rank", F.row_number().over(Window.partitionBy("hk_customer").orderBy(F.col("load_date").desc())),
        ).where(F.col("_rank") == 1).select(
            "hk_customer", F.col("_event_id").alias(label + "_event_id"),
            F.col("load_date").alias(label + "_load_date"),
        )
        if latest.join(hubs, "hk_customer", "left_anti").limit(1).count():
            raise ContractError("PIT satellite event has no matching as-of hub: " + label)
        if hubs.join(latest.select("hk_customer"), "hk_customer", "left_anti").limit(1).count():
            raise ContractError("PIT as-of hub lacks a committed satellite event: " + label)
        result = result.join(latest, "hk_customer", "inner")
    return result.select(
        F.lit(project_id).alias("project_id"), F.lit(batch_id).alias("batch_id"),
        "hk_customer", F.lit(as_of).alias("as_of"),
        "raw_event_id", "raw_load_date", "bv_event_id", "bv_load_date",
    ).withColumn("_pit_id", F.sha2(F.to_json(F.struct("hk_customer", "batch_id")), 256))


def write_pit(
    control: Any, owner: Any, *, project: ProjectRelease, delivery: dict[str, Any],
    now: datetime, proof_only: bool = False,
) -> dict[str, Any]:
    from datetime import timezone

    if proof_only is not True:
        raise ContractError("Customer PIT candidate requires explicit isolated proof scope")
    pit_spec(project)
    control.assert_owner(owner)
    batch = _proof_batch(control, project, delivery["batch_id"])
    cutoff = batch["load_date"]
    cutoff = cutoff.replace(tzinfo=timezone.utc) if cutoff.tzinfo is None else cutoff
    states = {}
    commits = []
    for parent in sorted(PIT_PARENTS):
        entity_id = parent.split(":", 1)[1]
        receipt = delivery["entities"][entity_id]
        states[entity_id] = committed_state(
            control, project=project, entity_id=entity_id, receipt=receipt, batch_id=delivery["batch_id"],
        )
        commits.extend([*receipt["history_commit_ids"], receipt["commit"]["commit_id"]])
    projected = project_pit(
        states["HUB_CUSTOMER"], states["SAT_CUSTOMER"], states["SAT_CUSTOMER_BV"],
        project_id=control.project, batch_id=delivery["batch_id"], as_of=cutoff,
    )
    output = write_immutable_output(
        control, owner, projected, role="processing", schema="business_vault",
        table="phase6_pit_customer", key="_pit_id",
        scope={"project_id": control.project, "batch_id": delivery["batch_id"]},
    )
    commit = commit_layer(
        control, owner, batch_id=delivery["batch_id"], layer="BUSINESS_VAULT", entity="PIT_CUSTOMER",
        input_fingerprint=fingerprint({
            "release_id": project.release_id, "pit_contract": PIT_CONTRACT, "input_commits": sorted(commits),
            "as_of": cutoff.isoformat(timespec="microseconds"),
        }),
        outputs={key: output[key] for key in ("target_table", "record_count", "output_fingerprint")},
        now=now,
    )
    return {"output": output, "commit": commit, "input_commit_ids": sorted(commits), "as_of": cutoff.isoformat()}
