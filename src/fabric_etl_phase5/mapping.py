"""Compile candidate metadata into batch-scoped typed Quality evaluation."""

from __future__ import annotations

import re
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any

from fabric_etl.contracts import ContractError, fingerprint, identifier, required_text
from fabric_etl.project import ProjectRelease, data_type
from fabric_etl_phase5.ingestion import RAW_RECORD_COLUMNS
from fabric_etl_phase5.quality import Rule, evaluate_typed_quality


def _quoted_end(sql: str, start: int) -> int:
    quote = sql[start]
    index = start + 1
    while index < len(sql):
        if sql[index] == "\\" and quote != "`":
            index += 2
        elif sql[index] == quote:
            if index + 1 < len(sql) and sql[index + 1] == quote:
                index += 2
            else:
                return index + 1
        else:
            index += 1
    raise ContractError("Unclosed quote in mapping expression")


def reject_invalid_casts(sql: str) -> tuple[str, list[str]]:
    """Replace explicit CASTs with TRY_CAST and retain every cast validity guard."""
    output: list[str] = []
    guards: list[str] = []
    index = 0
    while index < len(sql):
        if sql[index] in "'\"`":
            end = _quoted_end(sql, index)
            output.append(sql[index:end])
            index = end
            continue
        if sql.startswith("--", index) or sql.startswith("/*", index) or sql[index] == ";":
            raise ContractError("Comments/statements are not allowed in mapping expressions")
        match = re.match(r"(?i)cast\s*\(", sql[index:])
        if match and (index == 0 or not re.match(r"[\w]", sql[index - 1])):
            start = index + match.end()
            cursor, depth, separator = start, 0, None
            while cursor < len(sql):
                if sql[cursor] in "'\"`":
                    cursor = _quoted_end(sql, cursor)
                    continue
                if sql[cursor] == "(":
                    depth += 1
                elif sql[cursor] == ")":
                    if depth == 0:
                        break
                    depth -= 1
                elif depth == 0 and re.match(r"(?i)\sAS\s", sql[cursor:]):
                    separator = cursor
                cursor += 1
            if cursor == len(sql) or separator is None:
                raise ContractError("CAST mapping must declare a balanced AS datatype")
            operand, nested = reject_invalid_casts(sql[start:separator].strip())
            target = re.sub(r"(?i)^AS\s+", "", sql[separator:cursor].strip())
            target = data_type(target.upper().replace("BIGINT", "LONG"))
            cast = f"try_cast({operand} AS {target})"
            guards.extend(nested)
            guards.append(f"(({operand}) IS NULL OR {cast} IS NOT NULL)")
            output.append(cast)
            index = cursor + 1
        else:
            output.append(sql[index])
            index += 1
    return "".join(output), guards


def _sql_code(sql: str) -> str:
    output, index = [], 0
    while index < len(sql):
        if sql[index] in "'\"`":
            end = _quoted_end(sql, index)
            output.append(" " * (end - index))
            index = end
        else:
            output.append(sql[index])
            index += 1
    return "".join(output)


def quality_spec(project: ProjectRelease, source_id: str, *, reference_views: dict[str, str]) -> dict[str, Any]:
    document = project.document
    sources = {source["id"]: source for source in document["source_objects"]}
    if source_id not in sources:
        raise ContractError("Unknown metadata Quality source")
    source = sources[source_id]
    schema = next(schema for schema in document["schemas"] if schema["id"] == source["schema_id"])
    mappings = sorted(
        (mapping for mapping in document["mappings"]
         if mapping["entity_id"] == f"source_objects:{source_id}"),
        key=lambda mapping: mapping["ordinal"],
    )
    if [mapping["target_column"] for mapping in mappings] != [column["name"] for column in schema["columns"]]:
        raise ContractError("Quality mappings do not cover the declared output schema in order")
    projections, validation = [], []
    for index, mapping in enumerate(mappings):
        expression, guards = reject_invalid_casts(mapping["expression"])
        target_type = data_type(mapping["data_type"])
        projected = f"try_cast(({expression}) AS {target_type})"
        guards.append(f"(({expression}) IS NULL OR {projected} IS NOT NULL)")
        guard_name = f"_mapping_valid_{index}"
        projections.append({
            "column": mapping["target_column"], "expression": projected,
            "guard_column": guard_name, "guard_expression": " AND ".join(guards),
        })
        validation.append(Rule(f"CAST-{mapping['id']}", guard_name, "ERROR",
                               f"INVALID_CAST_{index}", 100, "CONTINUE"))
        if not mapping["nullable"]:
            validation.append(Rule(
                f"REQUIRED-{mapping['id']}", f"`{mapping['target_column']}` IS NOT NULL",
                "ERROR", f"REQUIRED_COLUMN_{index}", 100, "CONTINUE",
            ))
    filters = sorted(
        (item for item in document["filters"] if item["source_object_id"] == source_id),
        key=lambda item: item["ordinal"],
    )
    # Legacy filters describe records to keep; the Quality kernel describes rows to exclude.
    excluded = "NOT (" + " AND ".join(f"({item['expression']})" for item in filters) + ")" if filters else "FALSE"
    legacy_targets = {
        f"{{quality_catalog}}.{key.split('.')[0].lower()}.{value['targets']['quality']['table']}": key
        for key, value in sources.items()
    }
    rules = []
    used_references = set()
    for rule in sorted(
        (item for item in document["quality_rules"] if item["source_object_id"] == source_id),
        key=lambda item: item["ordinal"],
    ):
        expression = rule["expression"]
        if rule["id"] == "DQ-NAG-002":
            if expression != "holiday_types IS NOT NULL AND size(holiday_types) > 0":
                raise ContractError("Nager serialized-array rule differs from the reviewed source contract")
            expression = "holiday_types IS NOT NULL AND size(from_json(holiday_types, 'array<string>')) > 0"
        for match in reversed(list(re.finditer(r"(?i)\bcurrent_date\s*\(\s*\)", _sql_code(expression)))):
            expression = expression[:match.start()] + "cast(load_date_utc AS DATE)" + expression[match.end():]
        for target, reference_id in legacy_targets.items():
            if target in expression:
                if reference_id not in reference_views:
                    raise ContractError(f"Batch-scoped Quality reference is required: {reference_id}")
                view = identifier(reference_views[reference_id])
                expression = expression.replace(target, f"`{view}`")
                used_references.add(reference_id)
        if "{" in _sql_code(expression) or "}" in _sql_code(expression):
            raise ContractError(f"Unresolved Quality reference binding: {rule['id']}")
        action = {"BLOCK": "FAIL_BATCH", "REJECT": "CONTINUE", "ANNOTATE": "CONTINUE"}[rule["action"]]
        rules.append(Rule(rule["id"], expression, rule["severity"], rule["code"], rule["threshold_pct"], action))
    return {
        "source_id": source_id, "release_id": project.release_id,
        "columns": schema["columns"], "projections": projections,
        "excluded_expression": excluded, "rules": validation + rules,
        "metadata_rule_ids": [rule.rule_id for rule in rules],
        "reference_sources": sorted(used_references),
    }


def evaluate_metadata_quality(
    records: Any, *, source_schema: Any, project: ProjectRelease, source_id: str,
    reference_frames: dict[str, Any],
) -> tuple[Any, dict[str, Any]]:
    from pyspark.sql import functions as F
    from uuid import uuid4

    if records.columns != list(RAW_RECORD_COLUMNS):
        raise ContractError("Metadata Quality requires the exact Bronze contract")
    if records.where(F.col("source_object_id") != source_id).limit(1).count():
        raise ContractError("Metadata Quality input contains another source object")
    contexts = records.select("project_id", "source_system_id", "delivery_id", "batch_id", "release_id").distinct().collect()
    if len(contexts) != 1 or contexts[0]["project_id"] != project.document["project_id"]:
        raise ContractError("Metadata Quality requires one project/batch/delivery context")
    context = contexts[0]
    if context["release_id"] != project.release_id:
        raise ContractError("Metadata Quality record release differs from the candidate")
    views: dict[str, str] = {}
    try:
        for reference_id, reference in reference_frames.items():
            for column in ("project_id", "source_system_id", "delivery_id", "batch_id", "release_id"):
                if column not in reference.columns or reference.where(
                    F.col(column).isNull() | (F.col(column) != context[column]),
                ).limit(1).count():
                    raise ContractError("Quality reference does not belong to the same batch/release context")
            if "source_object_id" not in reference.columns or reference.where(
                F.col("source_object_id").isNull() | (F.col("source_object_id") != reference_id),
            ).limit(1).count():
                raise ContractError("Quality reference source identity differs from its binding")
            if "terminal_status" not in reference.columns or reference.where(
                F.col("terminal_status").isNull() | (F.col("terminal_status") != "PASSED"),
            ).limit(1).count():
                raise ContractError("Quality references must contain only PASSED outcomes")
            view = "_quality_ref_" + uuid4().hex
            reference.createOrReplaceTempView(view)
            views[reference_id] = view
        spec = quality_spec(project, source_id, reference_views=views)
        source_names = [field.name for field in source_schema.fields]
        if set(source_names).intersection(RAW_RECORD_COLUMNS) or any(name.startswith("_") for name in source_names):
            raise ContractError("Source payload collides with runtime columns")
        parsed = records.withColumn(
            "_source", F.from_json("payload_json", source_schema, {"mode": "FAILFAST"}),
        ).select(*RAW_RECORD_COLUMNS, "_source.*")
        projected = parsed.select(
            *RAW_RECORD_COLUMNS, F.expr(spec["excluded_expression"]).alias("_metadata_filtered"),
            *[F.expr(item["expression"]).alias(item["column"]) for item in spec["projections"]],
            *[F.expr(item["guard_expression"]).alias(item["guard_column"]) for item in spec["projections"]],
        )
        outcomes, accounting = evaluate_typed_quality(
            records, projected, payload_names=[item["name"] for item in spec["columns"]],
            rules=spec["rules"], filter_expression="_metadata_filtered",
        )
        accounting["release_id"] = project.release_id
        accounting["source_object_id"] = source_id
        accounting["metadata_rule_ids"] = spec["metadata_rule_ids"]
        return outcomes, accounting
    finally:
        for view in views.values():
            records.sparkSession.catalog.dropTempView(view)


def write_metadata_quality(
    control: Any, owner: Any, records: Any, *, source_schema: Any,
    project: ProjectRelease, source_id: str, bronze_commit_id: str,
    table: str, now: datetime, proof_only: bool = False,
) -> dict[str, Any]:
    """Commit reference-free metadata Quality only from reconciled, committed Bronze."""
    import json
    from pyspark.sql import functions as F
    from fabric_etl.control import literal, rows
    from fabric_etl_phase5 import __version__
    from fabric_etl_phase5.ingestion import _output_fingerprint
    from fabric_etl_phase5.quality import commit_layer, write_immutable_output

    identifier(table)
    required_text(bronze_commit_id, "Bronze commit ID")
    if control.project != project.document["project_id"]:
        raise ContractError("Metadata writer project differs from control ownership")
    # Missing referential bindings fail before writes; committed reference loading is a separate extension.
    spec = quality_spec(project, source_id, reference_views={})
    if records.columns != list(RAW_RECORD_COLUMNS):
        raise ContractError("Metadata writer requires the exact Bronze contract")
    context_columns = (
        "project_id", "source_system_id", "delivery_id", "batch_id",
        "release_id", "load_date_utc", "delivery_sequence", "source_object_id",
    )
    contexts = records.select(*context_columns).distinct().collect()
    if len(contexts) != 1 or any(contexts[0][name] is None for name in context_columns):
        raise ContractError("Metadata writer requires one nonempty, non-NULL persisted context")
    context = contexts[0].asDict()
    expected_source = "PHASE4_PROOF" if proof_only else source_id.split(".")[0]
    if type(proof_only) is not bool or (proof_only and not table.startswith("phase5_metadata_")):
        raise ContractError("Metadata proof writes require an isolated table prefix")
    if (
        context["project_id"] != control.project
        or context["release_id"] != project.release_id
        or context["source_object_id"] != source_id
        or context["source_system_id"] != expected_source
    ):
        raise ContractError("Metadata writer input scope differs from the candidate source")
    control.assert_owner(owner)
    batches = rows(control.spark, f"SELECT * FROM {control.table('batch_context')} "
                   f"WHERE batch_id = {literal(context['batch_id'])}")
    if len(batches) != 1 or any(batches[0].get(name) != context[name] for name in (
        "project_id", "source_system_id", "delivery_id", "delivery_sequence",
    )) or batches[0].get("release_id") != project.release_id:
        raise ContractError("Metadata writer context differs from persisted batch context")
    loaded = batches[0]["load_date"]
    loaded = loaded.replace(tzinfo=timezone.utc) if loaded.tzinfo is None else loaded.astimezone(timezone.utc)
    if loaded.isoformat(timespec="microseconds") != context["load_date_utc"]:
        raise ContractError("Metadata writer timestamp differs from persisted batch context")
    if not proof_only:
        active = rows(control.spark, f"SELECT * FROM {control.table('metadata_pointer')} "
                      f"WHERE project_id = {literal(control.project)}")
        if len(active) != 1 or active[0].get("active_release_id") != project.release_id:
            raise ContractError("Metadata business writes require the active approved release")
    commits = rows(control.spark, f"SELECT * FROM {control.table('commit_manifest')} "
                   f"WHERE commit_id = {literal(bronze_commit_id)}")
    if (
        len(commits) != 1 or commits[0].get("status") != "COMMITTED"
        or commits[0].get("layer") != "BRONZE"
        or commits[0].get("batch_id") != context["batch_id"]
        or commits[0].get("entity_id") != context["source_system_id"] + "." + context["delivery_id"]
    ):
        raise ContractError("Metadata writer requires the committed Bronze delivery")
    bronze = json.loads(commits[0]["outputs_json"])
    if not isinstance(bronze, dict) or set(bronze) != {"target_table", "record_count", "output_fingerprint"}:
        raise ContractError("Metadata writer requires a single-target Bronze commit contract")
    target = bronze["target_table"]
    prefix = control.environment.spark_table("processing", "bronze", table).rsplit(".", 1)[0] + "."
    if not isinstance(target, str) or not target.startswith(prefix):
        raise ContractError("Committed Bronze target is outside the processing Bronze namespace")
    suffix = target[len(prefix):]
    if not re.fullmatch(r"`[A-Za-z_][A-Za-z0-9_]*`", suffix):
        raise ContractError("Committed Bronze target name is not a quoted identifier")
    stored = control.spark.table(target)
    scope = {name: context[name] for name in (
        "project_id", "source_system_id", "delivery_id", "batch_id", "release_id", "source_object_id",
    )}
    condition = F.lit(True)
    for name, value in scope.items():
        condition = condition & (F.col(name) == value)
    stored = stored.where(condition).select(*RAW_RECORD_COLUMNS)
    if records.exceptAll(stored).limit(1).count() or stored.exceptAll(records).limit(1).count():
        raise ContractError("Metadata writer input differs from durable Bronze readback")
    count, digest = _output_fingerprint(stored)
    if count != bronze["record_count"] or digest != bronze["output_fingerprint"]:
        raise ContractError("Durable Bronze readback differs from its commit fingerprint")
    outcomes, accounting = evaluate_metadata_quality(
        records, source_schema=source_schema, project=project, source_id=source_id, reference_frames={},
    )
    outcomes = outcomes.cache()
    try:
        outputs = {}
        for status, schema, name in (
            ("PASSED", "quality", table), ("REJECTED", "reject", table),
            ("FILTERED", "quality", table + "_filtered"),
        ):
            outputs[status] = write_immutable_output(
                control, owner, outcomes.where(F.col("terminal_status") == status),
                role="processing", schema=schema, table=name, key="raw_record_id", scope=scope,
            )
        stable = {
            status: {key: output[key] for key in ("target_table", "record_count", "output_fingerprint")}
            for status, output in outputs.items()
        }
        commit = commit_layer(
            control, owner, batch_id=context["batch_id"], layer="QUALITY", entity=source_id,
            input_fingerprint=fingerprint({
                "bronze_commit_id": bronze_commit_id, "release_id": project.release_id,
                "source_id": source_id, "source_schema": source_schema.json(),
                "projections": spec["projections"], "rules": [asdict(rule) for rule in spec["rules"]],
                "filter": spec["excluded_expression"], "runtime": __version__,
            }),
            outputs={"targets": stable, "accounting": accounting}, now=now,
        )
        return {"accounting": accounting, "outputs": outputs, "quality_commit": commit}
    finally:
        outcomes.unpersist()
