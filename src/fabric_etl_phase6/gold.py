"""Isolated Sales Gold candidates from committed Vault state and pinned SQL."""

from __future__ import annotations

import hashlib
import re
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from fabric_etl.contracts import ContractError, fingerprint
from fabric_etl.project import ProjectRelease
from fabric_etl_phase5.quality import commit_layer, write_immutable_output
from fabric_etl_phase6.pit import committed_state
from fabric_etl_phase6.vault import _proof_batch, event_intervals, vault_order, vault_spec


GOLD_CONTRACT = "sales-gold-v1-committed-multistream-timelines"
BINDING = re.compile(r"\{(processing|gold)\}\.(raw_vault|business_vault|historical)\.([a-z_][a-z_0-9]*)")
AUDIT_COLUMNS = {
    "HISTORICAL": {"_batch_id": "STRING", "_loaded_at": "TIMESTAMP"},
    "CURRENT": {"_as_of_delivery_id": "STRING", "_as_of_timestamp": "TIMESTAMP", "_batch_id": "STRING"},
}


def gold_columns(project: ProjectRelease, entity: dict[str, Any], *, business_only: bool) -> list[dict[str, Any]]:
    columns = next(schema["columns"] for schema in project.document["schemas"] if schema["id"] == entity["schema_id"])
    declared = {column["name"]: column["data_type"] for column in columns if column["name"].startswith("_")}
    if declared != AUDIT_COLUMNS[entity["layer"]]:
        raise ContractError("Gold declared runtime audit columns differ from the registered layer contract")
    return [column for column in columns if not business_only or not column["name"].startswith("_")]


def sales_gold_entities(project: ProjectRelease) -> list[dict[str, Any]]:
    products = [product for product in project.document["products"] if product["id"] == "SALES_MART"]
    if len(products) != 1:
        raise ContractError("Sales Gold requires its exact registered data product")
    inventory = products[0]["entities"]
    entities = [entity for entity in project.document["gold_entities"] if entity["id"] in inventory]
    if len(inventory) != 11 or len(set(inventory)) != 11 or len(entities) != 11:
        raise ContractError("Sales Gold requires all eleven product entities")
    if any(entity["source_system_id"] != "SALES" or entity["product_id"] != "SALES_MART"
           or entity["temporal_policy"] != "COMMITTED_AS_OF" for entity in entities):
        raise ContractError("Sales Gold source, product or as-of policy differs")
    return sorted(entities, key=lambda entity: (entity["layer"] != "HISTORICAL", entity["id"]))


def gold_sql(project: ProjectRelease, sources: dict[str, str]) -> dict[str, str]:
    entities = sales_gold_entities(project)
    expressions = {expression["id"]: expression for expression in project.document["expressions"]}
    required = {entity["expression_ref"] for entity in entities}
    if set(sources) != required:
        raise ContractError("Sales Gold SQL inventory must match all product expressions")
    queries = {}
    for entity in entities:
        expression = expressions[entity["expression_ref"]]
        query = sources[expression["id"]]
        if expression["engine"] != "SPARK_SQL" or (
            hashlib.sha256(query.encode("utf-8")).hexdigest() != project.document["files"][expression["path"]]
        ):
            raise ContractError("Sales Gold SQL engine or source checksum differs")
        if entity["id"] == "GH_FCT_SALES":
            for alias in ("bv", "so"):
                old = f"AND {alias}.is_current"
                if query.count(old) != 1:
                    raise ContractError("Sales historical SQL differs from the reviewed temporal-overlay input")
                query = query.replace(old, (
                    f"AND sol.load_date >= {alias}.load_date AND sol.load_date < "
                    f"coalesce({alias}.load_end_date, timestamp'9999-12-31 23:59:59')"
                ))
            query += " AND loe.load_date <= sol.load_date"
            query = query.replace(
                "loc.hk_order = lop.hk_order LEFT JOIN",
                "loc.hk_order = lop.hk_order AND loc.load_date <= sol.load_date LEFT JOIN",
            )
        elif entity["id"] == "GH_FCT_RETURNS":
            query += " AND lre.load_date <= sr.load_date"
            query = query.replace(
                "loc.hk_order = lrop.hk_order LEFT JOIN",
                "loc.hk_order = lrop.hk_order AND loc.load_date <= sr.load_date LEFT JOIN",
            )
        queries[entity["id"]] = query
    return queries


def bind_gold_sql(query: str, bindings: dict[tuple[str, str, str], str]) -> str:
    from fabric_etl.contracts import identifier

    def replace(match: re.Match[str]) -> str:
        key = match.groups()
        if key not in bindings:
            raise ContractError("Gold query requires an unavailable committed view: " + ".".join(key))
        return "`" + identifier(bindings[key]) + "`"

    result = BINDING.sub(replace, query)
    if "{" in result or "}" in result:
        raise ContractError("Gold query has an unresolved namespace binding")
    return result


def expand_timeline(satellite: Any, boundaries: list[Any], *, key: str) -> Any:
    from pyspark.sql import functions as F

    events = satellite.select(key, "load_date")
    for frame in boundaries:
        events = events.unionByName(frame.select(key, "load_date"))
    start = satellite.groupBy(key).agg(F.min("load_date").alias("_first"))
    points = events.distinct().join(start, key).where(F.col("load_date") >= F.col("_first")).drop("_first")
    timeline = event_intervals(points, key=key).alias("point")
    values = event_intervals(satellite, key=key).alias("value")
    joined = timeline.join(
        values,
        (F.col("point." + key) == F.col("value." + key))
        & (F.col("point.load_date") >= F.col("value.load_date"))
        & (F.col("value.load_end_date").isNull() | (F.col("point.load_date") < F.col("value.load_end_date"))),
        "inner",
    )
    return joined.select(
        *[F.col("value." + column).alias(column) for column in satellite.columns if column != "load_date"],
        F.col("point.load_date").alias("load_date"),
        F.col("point.load_end_date").alias("load_end_date"), F.col("point.is_current").alias("is_current"),
    )


def project_gold(
    spark: Any, *, project: ProjectRelease, states: dict[str, Any], sources: dict[str, str],
) -> tuple[dict[str, Any], dict[str, str]]:
    from pyspark.sql import functions as F
    from fabric_etl_phase5.ingestion import spark_schema

    entities = sales_gold_entities(project)
    queries = gold_sql(project, sources)
    required = set(vault_order(project) + vault_order(project, business=True))
    if set(states) != required:
        raise ContractError("Sales Gold requires the complete committed Raw/BV inventory")
    for entity_id, key in (("LNK_ORDER_CUSTOMER", "hk_order"), ("LNK_ORDER_EMPLOYEE", "hk_order"),
                           ("LNK_RETURN_EMPLOYEE", "hk_return")):
        if states[entity_id].groupBy(key).count().where(F.col("count") != 1).limit(1).count():
            raise ContractError("Sales Gold relationship changes require an explicit effectivity contract: " + entity_id)
    prepared = {}
    for entity_id, frame in states.items():
        spec = vault_spec(project, entity_id, business=entity_id.endswith("_BV"))
        prepared[entity_id] = event_intervals(frame, key=spec["key_column"]) if spec["entity"]["type"] not in {"HUB", "LINK"} else frame
    prepared["SAT_CUSTOMER"] = expand_timeline(
        states["SAT_CUSTOMER"], [states["SAT_CUSTOMER_BV"]], key="hk_customer",
    )
    order_points = states["SAT_ORDER"].select("hk_order", "load_date").join(
        states["LNK_ORDER_PRODUCT"].select("hk_order", "hk_order_product"), "hk_order",
    ).select("hk_order_product", "load_date")
    prepared["SAT_ORDER_LINE"] = expand_timeline(
        states["SAT_ORDER_LINE"], [states["SAT_ORDER_LINE_BV"], order_points], key="hk_order_product",
    )
    bindings = {}
    views = []
    outputs = {}
    try:
        for entity_id, frame in prepared.items():
            view = "_phase6_gold_" + uuid4().hex
            frame.createOrReplaceTempView(view)
            views.append(view)
            schema = "business_vault" if entity_id.endswith("_BV") else "raw_vault"
            bindings[("processing", schema, entity_id.lower())] = view
        for entity in entities:
            frame = spark.sql(bind_gold_sql(queries[entity["id"]], bindings))
            columns = gold_columns(project, entity, business_only=True)
            expected_schema = spark_schema(columns)
            if [(field.name, field.dataType) for field in frame.schema.fields] != [
                (field.name, field.dataType) for field in expected_schema.fields
            ]:
                raise ContractError("Gold output schema differs from the declared entity: " + entity["id"])
            for column in columns:
                if not column["nullable"] and frame.where(F.col(column["name"]).isNull()).limit(1).count():
                    raise ContractError("Gold required output column is NULL: " + entity["id"] + "." + column["name"])
            if frame.groupBy(*entity["keys"]).count().where(F.col("count") != 1).limit(1).count():
                raise ContractError("Gold declared business grain is duplicated: " + entity["id"])
            outputs[entity["id"]] = frame
            if entity["layer"] == "HISTORICAL":
                view = "_phase6_gold_" + uuid4().hex
                frame.createOrReplaceTempView(view)
                views.append(view)
                bindings[("gold", "historical", entity["target"]["table"])] = view
        for fact_id in ("GH_FCT_SALES", "GH_FCT_RETURNS", "GC_FCT_SALES", "GC_FCT_RETURNS"):
            frame = outputs[fact_id]
            for dimension_id, column in (
                ("GH_DIM_CUSTOMER", "customer_hk"), ("GH_DIM_PRODUCT", "product_hk"),
                ("GH_DIM_EMPLOYEE", "employee_hk"),
            ):
                keys = outputs[dimension_id].select(column).distinct()
                if frame.where(F.col(column).isNotNull()).join(keys, column, "left_anti").limit(1).count():
                    raise ContractError("Gold fact has no matching historical dimension identity: " + fact_id + "." + column)
            date_keys = outputs["GC_DIM_DATE"].select("date_key")
            for column in frame.columns:
                if column.endswith("_date_key"):
                    if frame.where(F.col(column).isNotNull()).join(
                        date_keys.withColumnRenamed("date_key", column), column, "left_anti",
                    ).limit(1).count():
                        raise ContractError("Gold fact date is outside the registered calendar: " + fact_id + "." + column)
    finally:
        for view in views:
            spark.catalog.dropTempView(view)
    return outputs, {entity: hashlib.sha256(query.encode()).hexdigest() for entity, query in queries.items()}


def write_gold(
    control: Any, owner: Any, *, project: ProjectRelease, delivery: dict[str, Any],
    sources: dict[str, str], now: datetime, proof_only: bool = False,
    stop_after_entity: str | None = None,
) -> dict[str, Any]:
    from pyspark.sql import functions as F

    if proof_only is not True:
        raise ContractError("Sales Gold candidate requires explicit isolated proof scope")
    entities = sales_gold_entities(project)
    if stop_after_entity is not None and stop_after_entity not in {entity["id"] for entity in entities}:
        raise ContractError("Gold interruption requires a registered candidate entity")
    control.assert_owner(owner)
    batch = _proof_batch(control, project, delivery["batch_id"])
    cutoff = batch["load_date"]
    cutoff = cutoff.replace(tzinfo=timezone.utc) if cutoff.tzinfo is None else cutoff
    states, commits = {}, []
    for entity_id in vault_order(project) + vault_order(project, business=True):
        receipt = delivery["entities"][entity_id]
        states[entity_id] = committed_state(
            control, project=project, entity_id=entity_id, receipt=receipt, batch_id=delivery["batch_id"],
        )
        commits.extend([*receipt["history_commit_ids"], receipt["commit"]["commit_id"]])
    frames, query_hashes = project_gold(control.spark, project=project, states=states, sources=sources)
    product_release_id = fingerprint({
        "project_release_id": project.release_id, "input_commits": sorted(commits),
        "batch_id": delivery["batch_id"], "gold_contract": GOLD_CONTRACT, "query_sha256": query_hashes,
    })
    outputs = {}
    for entity in entities:
        entity_id = entity["id"]
        frame = frames[entity_id].withColumn("_batch_id", F.lit(delivery["batch_id"]))
        if entity["layer"] == "HISTORICAL":
            frame = frame.withColumn("_loaded_at", F.lit(cutoff))
        else:
            frame = frame.withColumn("_as_of_delivery_id", F.lit(batch["delivery_id"])).withColumn(
                "_as_of_timestamp", F.lit(cutoff),
            )
        frame = frame.select(*[column["name"] for column in gold_columns(project, entity, business_only=False)])
        frame = frame.withColumn("project_id", F.lit(control.project)).withColumn(
            "batch_id", F.lit(delivery["batch_id"]),
        ).withColumn("product_release_id", F.lit(product_release_id)).withColumn(
            "_gold_id", F.sha2(F.to_json(F.struct("product_release_id", *entity["keys"])), 256),
        )
        outputs[entity_id] = write_immutable_output(
            control, owner, frame, role="gold", schema="historical" if entity["layer"] == "HISTORICAL" else "current",
            table="phase6_" + entity_id.lower(), key="_gold_id",
            scope={"project_id": control.project, "batch_id": delivery["batch_id"]},
        )
        if stop_after_entity == entity_id:
            raise ContractError("Controlled Gold interruption after immutable output: " + entity_id)
    commit = commit_layer(
        control, owner, batch_id=delivery["batch_id"], layer="GOLD", entity="PHASE6_SALES_MART",
        input_fingerprint=product_release_id,
        outputs={"product_release_id": product_release_id, "targets": {
            entity: {key: output[key] for key in ("target_table", "record_count", "output_fingerprint")}
            for entity, output in outputs.items()
        }},
        now=now,
    )
    return {
        "product_release_id": product_release_id, "outputs": outputs, "commit": commit,
        "input_commit_ids": sorted(commits), "query_sha256": query_hashes,
        "cutoff": cutoff.isoformat(), "consumer_published": False,
    }
