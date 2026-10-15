"""Metadata projections and committed, append-only Vault event histories."""

from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Any

from fabric_etl.canonical import spark_scalar
from fabric_etl.contracts import ContractError, fingerprint, hash_key, identifier
from fabric_etl.control import literal, rows
from fabric_etl.project import ProjectRelease
from fabric_etl_phase5.mapping import reject_invalid_casts
from fabric_etl_phase5.quality import commit_layer, frame_fingerprint, write_immutable_output
from fabric_etl_phase6.quality import committed_output


PROJECTION_CONTRACT = "sales-vault-events-v1-persisted-business-date"


def business_expression(expression: str) -> str:
    parts = re.split(r"('(?:[^']|'')*'|\"(?:[^\"]|\"\")*\"|`(?:[^`]|``)*`)", expression)
    return "".join(
        part if index % 2 else re.sub(
            r"(?i)(?<![\w.])current_date\s*\(\s*\)", "cast(load_date AS DATE)", part,
        ) for index, part in enumerate(parts)
    )


def vault_order(project: ProjectRelease, *, business: bool = False) -> list[str]:
    collection = "business_entities" if business else "vault_entities"
    specs = {
        entity["id"]: vault_spec(project, entity["id"], business=business)
        for entity in project.document[collection]
        if (entity["type"] != "PIT" and business) or (
            not business and entity["source_objects"]
            and all(source.startswith("SALES.") for source in entity["source_objects"])
        )
    }
    dependencies = {
        entity_id: {
            parent.split(":", 1)[1] for parent in spec["entity"]["parents"]
            if parent.startswith(collection + ":")
        } for entity_id, spec in specs.items()
    }
    if any(not parents.issubset(specs) for parents in dependencies.values()):
        raise ContractError("Vault scope omits a declared identity parent")
    ordered: list[str] = []
    while len(ordered) < len(specs):
        ready = sorted(key for key, parents in dependencies.items()
                       if key not in ordered and parents.issubset(ordered))
        if not ready:
            raise ContractError("Vault identity parents contain a dependency cycle")
        ordered.extend(ready)
    return ordered


def _proof_batch(control: Any, project: ProjectRelease, batch_id: str) -> dict[str, Any]:
    batches = rows(control.spark, f"SELECT * FROM {control.table('batch_context')} "
                   f"WHERE batch_id = {literal(batch_id)}")
    if len(batches) != 1 or any(batches[0].get(key) != value for key, value in {
        "project_id": control.project, "source_system_id": "PHASE4_PROOF",
        "release_id": project.release_id,
    }.items()):
        raise ContractError("Vault requires the persisted proof batch of this candidate")
    return batches[0]


def vault_spec(project: ProjectRelease, entity_id: str, *, business: bool = False) -> dict[str, Any]:
    collection = "business_entities" if business else "vault_entities"
    entities = {entity["id"]: entity for entity in project.document[collection]}
    if entity_id not in entities:
        raise ContractError("Unknown Vault entity")
    entity = entities[entity_id]
    mappings = sorted(
        (mapping for mapping in project.document["mappings"] if mapping["entity_id"] == collection + ":" + entity_id),
        key=lambda mapping: mapping["ordinal"],
    )
    if entity["type"] == "PIT":
        raise ContractError("PIT requires its explicit committed-event projection, not a satellite fallback")
    source_ids = {mapping["source_object_id"] for mapping in mappings}
    if len(source_ids) != 1 or None in source_ids:
        raise ContractError("Vault projection requires one declared mapping source")
    source_id = next(iter(source_ids))
    if not source_id.startswith("SALES."):
        raise ContractError("Sales Vault extension does not silently process another source system")
    all_entities = {entity["id"]: entity for entity in project.document["vault_entities"]}
    if entity["type"] in {"HUB", "LINK"}:
        identity = entity
    else:
        if len(entity["parents"]) != 1 or not entity["parents"][0].startswith("vault_entities:"):
            raise ContractError("Satellite requires one declared Raw Vault parent")
        parent_id = entity["parents"][0].split(":", 1)[1]
        if parent_id not in all_entities:
            raise ContractError("Vault scope omits a declared identity parent")
        identity = all_entities[parent_id]
        if identity["type"] not in {"HUB", "LINK"}:
            raise ContractError("Satellite identity parent must be a hub or link")
    prefix = "HUB_" if identity["type"] == "HUB" else "LNK_"
    if not identity["id"].startswith(prefix):
        raise ContractError("Vault identity does not follow the registered Sales hash-column convention")
    key_column = identifier("hk_" + identity["id"][len(prefix):].lower())
    schema = next(schema for schema in project.document["schemas"] if schema["id"] == entity["schema_id"])
    quality_schema = next(schema for schema in project.document["schemas"] if schema["id"] == next(
        source["schema_id"] for source in project.document["source_objects"] if source["id"] == source_id
    ))
    quality_types = {column["name"]: column["data_type"] for column in quality_schema["columns"]}
    if not set(identity["keys"]).issubset(quality_types):
        raise ContractError("Vault identity keys are missing from the typed Quality source")
    hash_columns = [mapping["target_column"] for mapping in mappings if mapping["hashdiff"]]
    if not business and entity["type"] in {"SATELLITE", "LINK_SATELLITE"} and hash_columns != entity["hash_columns"]:
        raise ContractError("Satellite hashdiff order differs between entity and mappings")
    if entity["type"] in {"SATELLITE", "LINK_SATELLITE"} and not hash_columns:
        raise ContractError("Satellite needs an explicit ordered hashdiff payload")
    return {
        "entity": entity, "source_id": source_id, "mappings": mappings,
        "schema_columns": schema["columns"], "identity": identity, "key_column": key_column,
        "identity_columns": [(key, quality_types[key]) for key in identity["keys"]],
        "hash_columns": hash_columns,
    }


def project_vault(frame: Any, project: ProjectRelease, entity_id: str, *, business: bool = False) -> Any:
    from pyspark.sql import functions as F

    spec = vault_spec(project, entity_id, business=business)
    entity = spec["entity"]
    if not {"project_id", "batch_id", "load_date_utc"}.issubset(frame.columns):
        raise ContractError("Vault requires persisted source context")
    key_hash = F.udf(lambda values: hash_key("SALES", values), "string")
    parent_hash = key_hash(F.array(*[
        F.expr(spark_scalar(column, kind)) for column, kind in spec["identity_columns"]
    ]))
    source = frame.withColumn("load_date", F.to_timestamp("load_date_utc"))
    for column, kind in spec["identity_columns"]:
        invalid = F.col(column).isNull()
        if kind == "STRING":
            invalid = invalid | (F.length(F.trim(F.col(column))) == 0)
        if source.where(invalid).limit(1).count():
            raise ContractError("Vault required identity column is NULL or empty: " + column)
    columns, guards = {}, []
    for mapping in spec["mappings"]:
        expression = mapping["expression"]
        if business:
            expression = business_expression(expression)
        expression, validity = reject_invalid_casts(expression)
        cast = f"try_cast(({expression}) AS {mapping['data_type']})"
        guards.extend(validity)
        guards.append(f"(({expression}) IS NULL OR {cast} IS NOT NULL)")
        if not mapping["nullable"]:
            guards.append(f"{cast} IS NOT NULL")
        if entity["type"] == "LINK" and mapping["role"] in {"BUSINESS_KEY", "DRIVING_KEY"}:
            columns[mapping["target_column"]] = key_hash(F.array(
                F.expr(spark_scalar(mapping["expression"], mapping["data_type"])),
            ))
        else:
            columns[mapping["target_column"]] = F.expr(cast)
    if guards and source.where(~F.coalesce(F.expr(" AND ".join(guards)), F.lit(False))).limit(1).count():
        raise ContractError("Vault mapping cast or required column failed: " + entity_id)
    columns[spec["key_column"]] = parent_hash
    if entity["type"] == "LINK":
        for key in entity["keys"]:
            if key not in columns:
                columns[key] = F.col(key)
    output = source.select(
        "project_id", "batch_id", "load_date", F.lit("SALES").alias("record_source"),
        *[column.alias(name) for name, column in columns.items()],
    )
    if spec["hash_columns"]:
        types = {mapping["target_column"]: mapping["data_type"] for mapping in spec["mappings"]}
        hashdiff = F.udf(lambda values: hash_key("HASHDIFF", values), "string")
        output = output.withColumn("hashdiff", hashdiff(F.array(*[
            F.expr(spark_scalar(column, types[column])) for column in spec["hash_columns"]
        ])))
    payload = F.to_json(F.struct(*[F.col(name) for name in columns]), options={"ignoreNullFields": "false"})
    if output.groupBy(spec["key_column"]).agg(F.countDistinct(payload).alias("variants")).where(
        F.col("variants") != 1,
    ).limit(1).count():
        raise ContractError("Conflicting business payloads for one Vault key: " + entity_id)
    return output.dropDuplicates([spec["key_column"]])


def select_new_events(staged: Any, previous: Any, *, key: str, satellite: bool) -> Any:
    from pyspark.sql import Window, functions as F

    identifier(key)
    if satellite:
        if previous.groupBy(key, "load_date").count().where(F.col("count") != 1).limit(1).count():
            raise ContractError("Committed satellite has ambiguous event timestamps")
        latest = previous.withColumn(
            "_rank", F.row_number().over(Window.partitionBy(key).orderBy(F.col("load_date").desc())),
        ).where(F.col("_rank") == 1).select(
            key, F.col("hashdiff").alias("_previous_hashdiff"), F.col("load_date").alias("_previous_load"),
        )
        joined = staged.join(latest, key, "left")
        if joined.where(F.col("_previous_load") >= F.col("load_date")).limit(1).count():
            raise ContractError("Satellite event would rewrite or backdate committed technical history")
        return joined.where(
            F.col("_previous_hashdiff").isNull() | (F.col("hashdiff") != F.col("_previous_hashdiff")),
        ).select(*staged.columns)
    if previous.groupBy(key).count().where(F.col("count") != 1).limit(1).count():
        raise ContractError("Committed hub/link key is duplicated")
    payload_columns = [name for name in staged.columns if name not in {
        "project_id", "batch_id", "load_date", "record_source",
    }]
    known = staged.join(previous.select(key), key, "inner").select(*staged.columns)
    old_payload = previous.select(*payload_columns)
    if known.select(*payload_columns).exceptAll(old_payload).limit(1).count():
        raise ContractError("Existing hub/link identity has a conflicting business payload")
    return staged.join(previous.select(key), key, "left_anti").select(*staged.columns)


def event_intervals(events: Any, *, key: str) -> Any:
    from pyspark.sql import Window, functions as F

    identifier(key)
    if events.groupBy(key, "load_date").count().where(F.col("count") != 1).limit(1).count():
        raise ContractError("Historical event intervals require unique key/timestamp pairs")
    result = events.withColumn(
        "load_end_date", F.lead("load_date").over(Window.partitionBy(key).orderBy("load_date")),
    )
    return result.withColumn("is_current", F.col("load_end_date").isNull())


def write_vault(
    control: Any, owner: Any, staged: Any, previous: Any, *, project: ProjectRelease,
    entity_id: str, input_commit_ids: list[str], table: str, now: datetime,
    history_commit_ids: list[str], business: bool = False, proof_only: bool = False,
) -> dict[str, Any]:
    from pyspark.sql import functions as F

    if proof_only is not True or identifier(table) != "phase6_" + entity_id.lower():
        raise ContractError("Sales candidate Vault writes require isolated phase-six tables")
    spec = vault_spec(project, entity_id, business=business)
    contexts = staged.select("project_id", "batch_id").distinct().collect()
    if len(contexts) != 1 or contexts[0]["project_id"] != control.project:
        raise ContractError("Vault needs one nonempty persisted batch context")
    batch_id = contexts[0]["batch_id"]
    batch = _proof_batch(control, project, batch_id)
    if staged.where(
        F.col("load_date").isNull() | (F.col("load_date") != F.lit(batch["load_date"])),
    ).limit(1).count():
        raise ContractError("Vault event timestamp differs from the persisted batch timestamp")
    if not input_commit_ids or len(set(input_commit_ids)) != len(input_commit_ids):
        raise ContractError("Vault requires unique authoritative input commit IDs")
    control.assert_owner(owner)
    expected_inputs = {("QUALITY", spec["source_id"])} | {
        ("RAW_VAULT", parent.split(":", 1)[1]) for parent in spec["entity"]["parents"]
    }
    actual_inputs = []
    quality_commit_id = None
    for commit_id in input_commit_ids:
        stored = rows(control.spark, f"SELECT * FROM {control.table('commit_manifest')} "
                      f"WHERE commit_id = {literal(commit_id)}")
        if len(stored) != 1 or stored[0]["status"] != "COMMITTED" or stored[0]["batch_id"] != batch_id:
            raise ContractError("Vault input commit is not authoritative for this batch")
        actual_inputs.append((stored[0]["layer"], stored[0]["entity_id"]))
        if stored[0]["layer"] == "QUALITY" and stored[0]["entity_id"] == spec["source_id"]:
            quality_commit_id = commit_id
        elif (stored[0]["layer"], stored[0]["entity_id"]) in expected_inputs:
            parent_id = stored[0]["entity_id"]
            parent_table = "phase6_" + parent_id.lower()
            current_parent = read_vault_commit(
                control, project=project, entity_id=parent_id, table=parent_table,
                commit_id=commit_id, batch_id=batch_id,
            )
            parent_inventory = _prior_inventory(
                control, project=project, entity_id=parent_id, table=parent_table,
                before_batch_id=batch_id, business=False,
            )
            prior_parent = read_history(
                control, project=project, entity_id=parent_id, table=parent_table,
                commit_ids=parent_inventory, empty=current_parent.limit(0), before_batch_id=batch_id,
            )
            parent_key = vault_spec(project, parent_id)["key_column"]
            parent_state = prior_parent.unionByName(current_parent)
            if parent_state.groupBy(parent_key).count().where(F.col("count") != 1).limit(1).count():
                raise ContractError("Vault identity parent has duplicated committed keys")
            if staged.join(parent_state.select(parent_key), parent_key, "left_anti").limit(1).count():
                raise ContractError("Vault identity has no matching committed parent key")
    if set(actual_inputs) != expected_inputs or len(actual_inputs) != len(expected_inputs):
        raise ContractError("Vault input commits differ from the exact source and parent inventory")
    if quality_commit_id is None:
        raise ContractError("Vault requires its authoritative Quality source commit")
    quality_table = "phase6_sales_" + spec["source_id"].split(".")[1].lower()
    quality, _ = committed_output(
        control, commit_id=quality_commit_id, batch_id=batch_id, layer="QUALITY",
        entity=spec["source_id"], source_id=spec["source_id"],
        target=control.environment.spark_table("processing", "quality", quality_table),
        role="processing", schema="quality", table=quality_table,
        source_system_id="PHASE4_PROOF", delivery_id=batch["delivery_id"], release_id=project.release_id,
    )
    from fabric_etl_phase5.ingestion import spark_schema
    source_schema_id = next(source["schema_id"] for source in project.document["source_objects"]
                            if source["id"] == spec["source_id"])
    source_columns = next(schema["columns"] for schema in project.document["schemas"]
                          if schema["id"] == source_schema_id)
    typed = quality.withColumn(
        "_typed", F.from_json("typed_payload_json", spark_schema(source_columns), {"mode": "FAILFAST"}),
    ).select("project_id", "batch_id", "load_date_utc", "_typed.*")
    authoritative_staged = project_vault(typed, project, entity_id, business=business)
    if staged.columns != authoritative_staged.columns or (
        staged.exceptAll(authoritative_staged).limit(1).count()
        or authoritative_staged.exceptAll(staged).limit(1).count()
    ):
        raise ContractError("Vault projection differs from its authoritative Quality payload")
    authoritative = read_history(
        control, project=project, entity_id=entity_id, table=table,
        commit_ids=history_commit_ids, empty=previous.limit(0), before_batch_id=batch_id,
        business=business,
    )
    if previous.exceptAll(authoritative).limit(1).count() or authoritative.exceptAll(previous).limit(1).count():
        raise ContractError("Vault prior history differs from its authoritative commit inventory")
    events = select_new_events(
        staged, previous, key=spec["key_column"],
        satellite=spec["entity"]["type"] in {"SATELLITE", "LINK_SATELLITE"},
    ).withColumn("_event_id", F.sha2(F.to_json(F.struct(
        F.col(spec["key_column"]), F.col("batch_id"),
    )), 256))
    output = write_immutable_output(
        control, owner, events, role="processing", schema="business_vault" if business else "raw_vault",
        table=table, key="_event_id", scope={"project_id": control.project, "batch_id": batch_id},
    )
    stable = {key: output[key] for key in ("target_table", "record_count", "output_fingerprint")}
    previous_count, previous_digest = frame_fingerprint(previous, spec["key_column"] if not business and
        spec["entity"]["type"] in {"HUB", "LINK"} else "_event_id")
    commit = commit_layer(
        control, owner, batch_id=batch_id, layer="BUSINESS_VAULT" if business else "RAW_VAULT",
        entity=entity_id, input_fingerprint=fingerprint({
            "release_id": project.release_id, "input_commits": sorted(input_commit_ids),
            "projection_contract": PROJECTION_CONTRACT,
            "prior_history": [previous_count, previous_digest],
        }), outputs=stable, now=now,
    )
    return {"output": output, "commit": commit}


def read_vault_commit(
    control: Any, *, project: ProjectRelease, entity_id: str, table: str,
    commit_id: str, batch_id: str, business: bool = False,
) -> Any:
    from pyspark.sql import functions as F

    vault_spec(project, entity_id, business=business)
    _proof_batch(control, project, batch_id)
    if table != "phase6_" + entity_id.lower():
        raise ContractError("Vault committed read requires the exact isolated entity binding")
    target = control.environment.spark_table("processing", "business_vault" if business else "raw_vault", table)
    records = rows(control.spark, f"SELECT * FROM {control.table('commit_manifest')} "
                   f"WHERE commit_id = {literal(commit_id)}")
    if len(records) != 1 or any(records[0].get(key) != value for key, value in {
        "status": "COMMITTED", "entity_id": entity_id, "batch_id": batch_id,
        "layer": "BUSINESS_VAULT" if business else "RAW_VAULT",
    }.items()):
        raise ContractError("Vault read requires its exact authoritative manifest")
    output = json.loads(records[0]["outputs_json"])
    if output["target_table"] != target:
        raise ContractError("Vault committed target differs from its declared binding")
    frame = control.spark.table(target).where(
        (F.col("project_id") == control.project) & (F.col("batch_id") == batch_id),
    )
    count, digest = frame_fingerprint(frame, "_event_id")
    if count != output["record_count"] or digest != output["output_fingerprint"]:
        raise ContractError("Vault committed output count/fingerprint changed")
    return frame


def read_history(
    control: Any, *, project: ProjectRelease, entity_id: str, table: str,
    commit_ids: list[str], empty: Any, before_batch_id: str, business: bool = False,
) -> Any:
    from pyspark.sql import functions as F

    vault_spec(project, entity_id, business=business)
    if len(set(commit_ids)) != len(commit_ids):
        raise ContractError("Vault history commit inventory is duplicated")
    _proof_batch(control, project, before_batch_id)
    target = control.environment.spark_table("processing", "business_vault" if business else "raw_vault", identifier(table))
    layer = "BUSINESS_VAULT" if business else "RAW_VAULT"
    inventory = _prior_inventory(
        control, project=project, entity_id=entity_id, table=table,
        before_batch_id=before_batch_id, business=business,
    )
    if len(inventory) != len(commit_ids) or set(inventory) != set(commit_ids):
        raise ContractError("Vault history inventory omits or adds authoritative prior commits")
    history = empty
    for commit_id in commit_ids:
        records = rows(control.spark, f"SELECT * FROM {control.table('commit_manifest')} "
                       f"WHERE commit_id = {literal(commit_id)}")
        if len(records) != 1 or any(records[0].get(key) != value for key, value in {
            "status": "COMMITTED", "entity_id": entity_id,
            "layer": layer,
        }.items()):
            raise ContractError("Vault history requires its exact authoritative commits")
        output = json.loads(records[0]["outputs_json"])
        if output["target_table"] != target:
            raise ContractError("Vault history target differs from its commit")
        frame = control.spark.table(target).where(
            (F.col("project_id") == control.project) & (F.col("batch_id") == records[0]["batch_id"]),
        )
        if frame.columns != empty.columns:
            raise ContractError("Vault history schema projection must retain every committed column")
        count, digest = frame_fingerprint(frame, "_event_id")
        if count != output["record_count"] or digest != output["output_fingerprint"]:
            raise ContractError("Vault committed history count/fingerprint changed")
        history = history.unionByName(frame)
    return history


def _prior_inventory(
    control: Any, *, project: ProjectRelease, entity_id: str, table: str,
    before_batch_id: str, business: bool,
) -> list[str]:
    target = control.environment.spark_table("processing", "business_vault" if business else "raw_vault", identifier(table))
    layer = "BUSINESS_VAULT" if business else "RAW_VAULT"
    inventory = rows(control.spark, f"""
        SELECT c.commit_id FROM {control.table('commit_manifest')} c
        JOIN {control.table('batch_context')} b ON b.batch_id = c.batch_id
        JOIN {control.table('batch_context')} current_batch ON current_batch.batch_id = {literal(before_batch_id)}
        WHERE c.status = 'COMMITTED' AND c.layer = {literal(layer)}
        AND c.entity_id = {literal(entity_id)}
        AND get_json_object(c.outputs_json, '$.target_table') = {literal(target)}
        AND b.project_id = {literal(control.project)} AND b.source_system_id = 'PHASE4_PROOF'
        AND b.release_id = {literal(project.release_id)}
        AND b.delivery_sequence < current_batch.delivery_sequence
    """)
    return [item["commit_id"] for item in inventory]
