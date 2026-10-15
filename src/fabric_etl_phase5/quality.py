"""Deterministic Quality accounting and immutable, owner-gated layer commits."""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from fabric_etl.contracts import ContractError, canonical_json, fingerprint, identifier, required_text
from fabric_etl_phase5.ingestion import RAW_RECORD_COLUMNS


class QualityBlocked(ContractError):
    """A blocking threshold prevents downstream commits."""


@dataclass(frozen=True)
class Rule:
    rule_id: str
    expression: str
    severity: str
    reason_code: str
    max_violation_percent: float
    on_threshold_breach: str

    def __post_init__(self) -> None:
        if not isinstance(self.rule_id, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_.-]*", self.rule_id):
            raise ContractError("Invalid Quality rule identity")
        identifier(self.reason_code)
        required_text(self.expression, "rule expression")
        if self.severity not in {"ERROR", "WARNING"}:
            raise ContractError("Quality severity must be ERROR or WARNING")
        if (
            isinstance(self.max_violation_percent, bool)
            or not isinstance(self.max_violation_percent, (int, float))
            or not math.isfinite(self.max_violation_percent)
            or not 0 <= self.max_violation_percent <= 100
        ):
            raise ContractError("Quality threshold must be finite and between 0 and 100")
        if self.on_threshold_breach not in {"CONTINUE", "FAIL_BATCH", "QUARANTINE_BATCH"}:
            raise ContractError("Unknown Quality threshold action")


def threshold_results(
    rules: list[Rule], *, population: int, violations: dict[str, int],
) -> list[dict[str, Any]]:
    if type(population) is not int or population < 0:
        raise ContractError("Invalid Quality population")
    if len({rule.rule_id for rule in rules}) != len(rules):
        raise ContractError("Duplicate Quality rule identity")
    if set(violations) != {rule.rule_id for rule in rules}:
        raise ContractError("Quality violation inventory differs from declared rules")
    results = []
    blocked = []
    for rule in rules:
        count = violations[rule.rule_id]
        if type(count) is not int or not 0 <= count <= population:
            raise ContractError("Invalid Quality violation count")
        percent = 0.0 if population == 0 else 100.0 * count / population
        breached = percent > rule.max_violation_percent
        results.append({
            "rule_id": rule.rule_id, "population": population,
            "violations": count, "violation_percent": percent,
            "threshold_percent": rule.max_violation_percent, "breached": breached,
            "action": rule.on_threshold_breach,
        })
        if breached and rule.on_threshold_breach != "CONTINUE":
            blocked.append(f"{rule.rule_id}:{rule.on_threshold_breach}")
    if blocked:
        raise QualityBlocked("Quality thresholds blocked: " + ", ".join(blocked))
    return results


def evaluate_quality(
    records: Any, *, payload_schema: Any, rules: list[Rule],
    filter_expression: str | None = None,
) -> tuple[Any, dict[str, Any]]:
    from pyspark.sql import functions as F

    if records.columns != list(RAW_RECORD_COLUMNS):
        raise ContractError("Quality requires the exact Bronze record contract")
    if len({rule.rule_id for rule in rules}) != len(rules):
        raise ContractError("Duplicate Quality rule identity")
    payload_names = [field.name for field in payload_schema.fields]
    if set(payload_names).intersection(RAW_RECORD_COLUMNS) or any(
        name.startswith("_") for name in payload_names
    ):
        raise ContractError("Quality payload fields collide with runtime columns")
    parsed = records.withColumn(
        "_typed", F.from_json("payload_json", payload_schema, {"mode": "FAILFAST"}),
    ).select(*RAW_RECORD_COLUMNS, "_typed.*")
    return evaluate_typed_quality(
        records, parsed, payload_names=payload_names, rules=rules,
        filter_expression=filter_expression,
    )


def evaluate_typed_quality(
    records: Any, parsed: Any, *, payload_names: list[str], rules: list[Rule],
    filter_expression: str | None = None,
) -> tuple[Any, dict[str, Any]]:
    from pyspark.sql import functions as F
    from pyspark.sql.types import BooleanType

    if records.columns != list(RAW_RECORD_COLUMNS):
        raise ContractError("Quality requires the exact Bronze record contract")
    if len({rule.rule_id for rule in rules}) != len(rules):
        raise ContractError("Duplicate Quality rule identity")
    if not payload_names or len(set(payload_names)) != len(payload_names):
        raise ContractError("Typed Quality requires unique payload columns")
    if set(payload_names).intersection(RAW_RECORD_COLUMNS) or any(name.startswith("_") for name in payload_names):
        raise ContractError("Quality payload fields collide with runtime columns")
    if not set((*RAW_RECORD_COLUMNS, *payload_names)).issubset(parsed.columns):
        raise ContractError("Typed Quality frame is missing contract columns")
    empty_codes = F.array().cast("array<string>")
    frame = parsed.withColumn(
        "_filtered", F.lit(False) if filter_expression is None else F.expr(filter_expression),
    )
    if not isinstance(frame.schema["_filtered"].dataType, BooleanType):
        raise ContractError("Quality filter must return BOOLEAN")
    if frame.where(F.col("_filtered").isNull()).limit(1).count():
        raise ContractError("Quality filter returned NULL; define its presence semantics explicitly")
    filtered_frame = frame.where(F.col("_filtered"))
    frame = frame.where(~F.col("_filtered"))
    error_codes = empty_codes
    warning_codes = empty_codes
    for index, rule in enumerate(rules):
        column = f"_rule_{index}"
        frame = frame.withColumn(column, F.expr(rule.expression))
        if not isinstance(frame.schema[column].dataType, BooleanType):
            raise ContractError(f"Quality rule must return BOOLEAN: {rule.rule_id}")
        failed = ~F.coalesce(F.col(column), F.lit(False))
        codes = F.when(failed, F.array(F.lit(rule.reason_code))).otherwise(empty_codes)
        if rule.severity == "ERROR":
            error_codes = F.concat(error_codes, codes)
        else:
            warning_codes = F.concat(warning_codes, codes)
    measured = frame.agg(
        F.count(F.lit(1)).alias("_population"),
        *[F.count(F.when(~F.coalesce(F.col(f"_rule_{index}"), F.lit(False)), F.lit(1)))
          .alias(f"_violations_{index}") for index in range(len(rules))],
    ).first()
    population = measured["_population"]
    violations = {rule.rule_id: measured[f"_violations_{index}"] for index, rule in enumerate(rules)}
    metrics = threshold_results(rules, population=population, violations=violations)
    payload = F.to_json(
        F.struct(*[F.col(name) for name in payload_names]),
        options={"ignoreNullFields": "false"},
    ).alias("typed_payload_json")
    outcomes = frame.select(
        *RAW_RECORD_COLUMNS,
        payload,
        F.when(F.size(error_codes) > 0, F.lit("REJECTED"))
        .otherwise(F.lit("PASSED")).alias("terminal_status"),
        error_codes.alias("error_codes"),
        warning_codes.alias("warning_codes"),
    ).unionByName(filtered_frame.select(
        *RAW_RECORD_COLUMNS, payload,
        F.lit("FILTERED").alias("terminal_status"),
        empty_codes.alias("error_codes"), empty_codes.alias("warning_codes"),
    ))
    observed_counts = {row["terminal_status"]: row["count"] for row in outcomes.groupBy("terminal_status").count().collect()}
    counts = {status: observed_counts.get(status, 0) for status in ("FILTERED", "PASSED", "REJECTED")}
    count = records.count()
    if count != sum(counts.values()) or outcomes.select("raw_record_id").distinct().count() != count:
        raise ContractError("Quality terminal accounting lost or duplicated Bronze record IDs")
    if records.select("raw_record_id").exceptAll(outcomes.select("raw_record_id")).limit(1).count():
        raise ContractError("Quality terminal record identities differ from Bronze")
    return outcomes, {"bronze_count": count, "counts": counts, "rules": metrics}


def frame_fingerprint(frame: Any, key: str) -> tuple[int, str]:
    from pyspark.sql import functions as F

    identifier(key)
    buckets = (
        frame.withColumn("_row_hash", F.sha2(F.to_json(
            F.struct(*[F.col(name) for name in frame.columns]),
            options={"ignoreNullFields": "false"},
        ), 256))
        .withColumn("_bucket", F.substring(F.sha2(F.col(key), 256), 1, 2))
        .groupBy("_bucket").agg(
            F.count(F.lit(1)).alias("count"),
            F.sha2(F.concat_ws("", F.sort_array(F.collect_list("_row_hash"))), 256).alias("sha256"),
        ).orderBy("_bucket").collect()
    )
    return frame.count(), fingerprint([row.asDict() for row in buckets])


def write_immutable_output(
    control: Any, owner: Any, frame: Any, *, role: str, schema: str, table: str,
    key: str, scope: dict[str, str],
) -> dict[str, Any]:
    from pyspark.sql import functions as F

    if (role, schema) not in {
        ("processing", "quality"), ("processing", "reject"), ("processing", "audit"),
        ("processing", "raw_vault"), ("processing", "business_vault"),
        ("gold", "historical"), ("gold", "current"),
    }:
        raise ContractError("Unregistered immutable-output layer")
    identifier(table)
    identifier(key)
    if not scope or not set(scope).issubset(frame.columns) or key not in frame.columns:
        raise ContractError("Immutable output requires a declared key and delivery scope")
    condition = F.lit(True)
    for name, value in scope.items():
        identifier(name)
        required_text(value, "scope value")
        condition = condition & (F.col(name) == value)
    if frame.where(~F.coalesce(condition, F.lit(False))).limit(1).count():
        raise ContractError("Immutable input contains rows outside its declared scope")
    if frame.where(F.col(key).isNull()).limit(1).count():
        raise ContractError("Immutable output key cannot be NULL")
    if frame.groupBy(key).count().where(F.col("count") != 1).limit(1).count():
        raise ContractError("Immutable output key is duplicated")
    spark = control.spark
    target = control.environment.spark_table(role, schema, table)
    control.assert_owner(owner)
    spark.sql(f"CREATE TABLE IF NOT EXISTS {target} ({frame.schema.toDDL()}) USING DELTA").collect()
    stored = spark.table(target)
    if [(f.name, f.dataType) for f in stored.schema.fields] != [
        (f.name, f.dataType) for f in frame.schema.fields
    ]:
        raise ContractError("Immutable target schema differs from requested output")
    actual = stored.where(condition)
    if actual.groupBy(key).count().where(F.col("count") != 1).limit(1).count():
        raise ContractError("Stored immutable output key is duplicated")
    if actual.exceptAll(frame).limit(1).count():
        raise ContractError("Stored immutable output conflicts with deterministic input")
    missing = frame.join(actual.select(key), key, "left_anti").select(*frame.columns)
    inserted = missing.count()
    if inserted:
        control.assert_owner(owner)
        missing.write.format("delta").mode("append").saveAsTable(target)
    control.assert_owner(owner)
    observed = spark.table(target).where(condition)
    if frame.exceptAll(observed).limit(1).count() or observed.exceptAll(frame).limit(1).count():
        raise ContractError("Immutable output readback does not exactly match input")
    count, digest = frame_fingerprint(observed, key)
    return {
        "target_table": target, "record_count": count, "inserted_count": inserted,
        "retry_count": count - inserted, "output_fingerprint": digest,
    }


def commit_layer(
    control: Any, owner: Any, *, batch_id: str, layer: str, entity: str,
    input_fingerprint: str, outputs: dict[str, Any], now: datetime,
) -> dict[str, str]:
    from fabric_etl.control import literal, rows, timestamp

    identifier(layer)
    required_text(entity, "commit entity")
    required_text(input_fingerprint, "input fingerprint")
    stable_outputs = canonical_json(outputs).decode("utf-8")
    commit_id = fingerprint([control.project, batch_id, layer, entity, input_fingerprint, outputs])
    control.assert_owner(owner)
    table = control.table("commit_manifest")
    predicate = (
        f"batch_id = {literal(batch_id)} AND layer = {literal(layer)} "
        f"AND entity_id = {literal(entity)}"
    )
    existing = rows(control.spark, f"SELECT * FROM {table} WHERE {predicate}")
    if existing:
        if (
            len(existing) != 1 or existing[0]["commit_id"] != commit_id
            or existing[0]["input_fingerprint"] != input_fingerprint
            or existing[0]["outputs_json"] != stable_outputs
            or existing[0]["status"] != "COMMITTED"
        ):
            raise ContractError("Layer commit conflicts with an existing batch/entity")
        return {"commit_id": commit_id, "status": "VERIFIED"}
    control.spark.sql(
        f"INSERT INTO {table} SELECT {literal(commit_id)}, {literal(batch_id)}, "
        f"{literal(layer)}, {literal(entity)}, {owner.generation}, "
        f"{literal(input_fingerprint)}, {literal(stable_outputs)}, 'COMMITTED', {timestamp(now)}"
    ).collect()
    control.assert_owner(owner)
    observed = rows(control.spark, f"SELECT * FROM {table} WHERE {predicate}")
    if (
        len(observed) != 1 or observed[0]["commit_id"] != commit_id
        or observed[0]["outputs_json"] != stable_outputs or observed[0]["status"] != "COMMITTED"
    ):
        raise ContractError("Layer commit readback differs")
    return {"commit_id": commit_id, "status": "COMMITTED"}
