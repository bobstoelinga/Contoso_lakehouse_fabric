"""Explicit, repeatable conversion of the v2 example into a candidate release."""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any

from fabric_etl import __version__
from fabric_etl.contracts import ContractError, fingerprint, load_json
from fabric_etl.project import COLLECTIONS, ProjectRelease, data_type, node


def _quality_mapping_expression(mapping: dict[str, Any]) -> str:
    expression = mapping["source_expression"]
    if mapping["mapping_id"] != "M-QA-FAB-05":
        return expression
    if (
        mapping["source_object_id"] != "FABRIC_SALES.ORDER_LINES"
        or mapping["target_column"] != "total_due_amount"
        or mapping["target_data_type"] != "DECIMAL(18,4)"
        or expression != "cast(coalesce(total_due_amount, subtotal_amount + tax_amount + freight_amount) as decimal(18,4))"
    ):
        raise ContractError("Unexpected legacy Fabric Sales total-due mapping; review the source contract")
    return "cast(subtotal_amount + tax_amount + freight_amount as decimal(18,4))"


def _declared_tables(path: Path) -> dict[str, list[dict[str, Any]]]:
    """Read column declarations from the fixed v2 DDL grammar, not executable SQL."""
    text = re.sub(r"--[^\n]*", "", path.read_text(encoding="utf-8"))
    text = re.sub(r"COMMENT\s+'(?:[^']|'')*'", "", text)
    tables = {}
    for match in re.finditer(r"CREATE TABLE IF NOT EXISTS ([a-z_0-9]+)\s*\(", text):
        start = match.end()
        depth = 1
        end = start
        while depth and end < len(text):
            depth += (text[end] == "(") - (text[end] == ")")
            end += 1
        if depth:
            raise ContractError(f"Unclosed legacy table declaration: {match[1]}")
        body = text[start:end - 1]
        declarations = re.split(r",\s*(?![0-9]+\s*\))", body)
        columns = []
        for declaration in declarations:
            declaration = declaration.strip()
            if declaration.startswith("CONSTRAINT "):
                break
            column = re.fullmatch(
                r"([a-z_0-9]+)\s+(STRING|BOOLEAN|DATE|TIMESTAMP|INT|BIGINT|DOUBLE|"
                r"DECIMAL\([0-9]+,[0-9]+\)|ARRAY<STRING>)(\s+NOT NULL)?"
                r"(?:\s+COMMENT '(?:[^']|'')*')?", declaration,
            )
            if column is None:
                raise ContractError(f"Unsupported legacy column declaration: {declaration}")
            column_type = column[2].replace("BIGINT", "LONG")
            if column_type == "ARRAY<STRING>":
                if column[1] != "holiday_types" or not match[1].startswith("dim_nager_holiday"):
                    raise ContractError("Unexpected complex entity type requires projection review")
                column_type = "STRING"
            columns.append({
                "name": column[1], "source_name": column[1],
                "data_type": data_type(column_type),
                "nullable": column[3] is None, "ordinal": len(columns) + 1,
            })
        if not columns or match[1] in tables:
            raise ContractError(f"Missing or duplicate legacy table: {match[1]}")
        tables[match[1]] = columns
    return tables


def _source_objects(path: Path) -> list[dict[str, Any]]:
    duplicate_count = 0

    def checked(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        nonlocal duplicate_count
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                if (
                    key != "freshness_sla_hours" or result[key] != 26 or value != 24
                    or result.get("source_object_id") != "SALES.CUSTOMERS"
                ):
                    raise ContractError(f"Unexpected legacy duplicate field: {key}")
                duplicate_count += 1
            result[key] = value
        return result

    rows = json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=checked)
    if duplicate_count != 1:
        raise ContractError("Expected exactly the reviewed Customers freshness 26/24 duplicate")
    return [r for r in rows if r["is_active"]]


def convert(seed: Path, output: Path) -> dict[str, Any]:
    """Generate a review-gated candidate, never an activated runtime release."""
    def read(name: str) -> list[dict[str, Any]]:
        rows = load_json(seed / f"{name}.json")
        if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
            raise ContractError(f"Expected metadata object array: {name}")
        return [row for row in rows if row.get("is_active", True)]

    objects = _source_objects(seed / "meta_source_object.json")
    object_ids = {r["source_object_id"] for r in objects}
    systems = [r for r in read("meta_source_system") if r["source_system_id"] in {o["source_system_id"] for o in objects}]
    quality = [r for r in read("meta_mapping") if r["source_object_id"] in object_ids]
    for mapping in quality:
        if mapping["target_data_type"].upper() == "ARRAY<STRING>":
            if mapping["source_object_id"] != "NAGER.HOLIDAYS_NL" or mapping["target_column"] != "holiday_types":
                raise ContractError("Unexpected complex Quality type requires explicit projection review")
            mapping["source_expression"] = f"to_json({mapping['source_expression']})"
            mapping["target_data_type"] = "STRING"
    dv = read("meta_dv_entity")
    dv_map = [r for r in read("meta_dv_mapping") if r["source_object_id"] in object_ids or r["source_object_id"].startswith("BV.")]
    gold = [r for r in read("meta_gold_entity") if r.get("source_system_id") != "SHAREPOINT"]
    release: dict[str, Any] = {collection: [] for collection in sorted(COLLECTIONS)}
    release.update(
        project_id="contoso", contract_version="1.0", framework_version=__version__,
        hash_contract_version="fabric-dv-key-v1", schema_contract_version="1.0", files={},
    )
    generated_files: dict[str, str] = {}
    ddl_root = seed.parent.parent / "sql"
    table_contracts = {}
    for name, relative in (
        ("historical", "05_gold/50_gold_historical.sql"),
        ("current", "05_gold/51_gold_current.sql"),
    ):
        path = ddl_root.joinpath(*relative.split("/"))
        table_contracts[name] = _declared_tables(path)
        generated_files["fixtures/legacy_" + path.name] = path.read_text(encoding="utf-8")

    def target(role: str, schema: str, table: str) -> dict[str, str]:
        return dict(role=role, schema=schema, table=table)

    def delete(obj: dict) -> dict:
        return {
            "mode": obj.get("delete_semantics", "NONE"),
            "flag_column": obj.get("deleted_flag_column"),
            "absence": bool(obj.get("absence_means_delete", False)),
            "max_delete_pct": None,
        }

    def expression(key: str, sql: str) -> str:
        replacements = {
            "{vault_catalog}": "{processing}", "{gold_catalog}": "{gold}",
            "{quality_catalog}": "{processing}",
        }
        for old, new in replacements.items():
            sql = sql.replace(old, new)
        relative = f"sql/{key}.sql"
        generated_files[relative] = sql.rstrip() + "\n"
        release["expressions"].append({
            "id": key, "engine": "SPARK_SQL", "path": relative,
            "bindings": sorted(set(re.findall(r"\{([A-Za-z_][A-Za-z0-9_]*)\}", sql))),
            "review_status": "REQUIRES_REVIEW",
        })
        return key

    def schema(key: str, cols: list[dict], keys: list[str], declared: bool = False,
               table_columns: list[dict] | None = None) -> str:
        by_name: dict[str, dict] = {}
        for col in cols:
            name = col["target_column"]
            if name not in by_name:
                by_name[name] = {
                    "name": name, "source_name": col.get("source_column") or name,
                    "data_type": col["target_data_type"].upper().replace("BIGINT", "LONG"),
                    "nullable": col.get("is_nullable", name not in keys),
                    "ordinal": len(by_name) + 1,
                }
        for name in keys:
            if name not in by_name:
                by_name[name] = dict(name=name, source_name=name, data_type="STRING",
                                     nullable=False, ordinal=len(by_name) + 1)
        if table_columns is not None:
            by_name = {column["name"]: dict(column) for column in table_columns}
        if not by_name:
            raise ContractError(f"Cannot construct schema without columns: {key}")
        # Grain-only/entity schemas are declarations awaiting Spark compile,
        # not inferred output types certified as executable.
        release["schemas"].append({
            "id": key, "scope": "QUALITY" if declared else "ENTITY",
            "columns": list(by_name.values()),
            "status": "DECLARED" if declared else "REQUIRES_COMPILE",
        })
        return key

    governance = {r["source_system_id"]: r for r in read("meta_data_governance_policy")}
    for system in systems:
        key = system["source_system_id"]
        policy = governance[key]
        release["policies"].append({
            "id": f"POLICY_{key}", "classification": policy["pii_classification"],
            "owner": policy["data_owner"], "retention_days": None, "status": "DEVELOPMENT",
        })
        release["source_systems"].append({
            "id": key, "name": system["source_system_name"],
            "delivery_mode": system["delivery_frequency"], "owner": policy["data_owner"],
            "policy_id": f"POLICY_{key}",
        })
    connector_seeds = {r["source_object_id"]: r for r in read("meta_source_connector")}
    for obj in objects:
        key = obj["source_object_id"]
        qcols = sorted([r for r in quality if r["source_object_id"] == key],
                       key=lambda r: r["ordinal_position"])
        # Quality keys may use renamed source columns (ECB and Nager).
        source_to_target = {r["source_column"]: r["target_column"] for r in qcols}
        keys = [source_to_target.get(k, k) for k in obj["business_key_columns"]]
        schema_id = schema(f"QA_{key}", qcols, keys, True)
        connector = connector_seeds.get(key)
        connector_id = f"CONNECTOR_{key}"
        if connector is None:
            kind, endpoint, connection, params = "FILE_MANIFEST", None, None, {"pattern": obj["file_pattern"]}
            timeout, retries = 60, 0
        else:
            kind = connector["connector_type"]
            options = connector["request_options"]
            timeout, retries = int(options.get("timeout_seconds", 60)), int(options.get("max_retries", 3))
            endpoint = connector["endpoint_url"] if kind.startswith("HTTP") else None
            connection = "fabric_sales_source" if kind == "JDBC" else None
            if kind == "JDBC":
                query = expression("SOURCE_FABRIC_SALES", options["source_query"])
                params = {"source_query_ref": query}
            elif kind == "HTTP_JSON":
                params = {"records_key": connector["records_key"], "next_link_key": connector["next_link_key"]}
            else:
                params = {"encoding": "utf-8", "delimiter": ","}
        release["connectors"].append({
            "id": connector_id, "type": kind, "endpoint": endpoint,
            "connection_ref": connection, "parameters": params,
            "timeout_seconds": timeout, "max_retries": retries,
        })
        release["source_objects"].append({
            "id": key, "source_system_id": obj["source_system_id"],
            "connector_id": connector_id, "schema_id": schema_id, "keys": keys,
            "route": obj.get("processing_route", "RAW_VAULT"),
            "format": obj["file_format"].upper(), "load_strategy": obj["load_strategy"],
            "delete_policy": delete(obj), "mandatory": obj["is_mandatory_in_delivery"],
            "freshness_hours": obj["freshness_sla_hours"],
            "drift_policy": obj["schema_drift_policy"],
            "targets": {layer: target("processing", layer, obj[f"{layer}_table"])
                        for layer in ("bronze", "quality", "reject")},
        })
        if obj.get("quality_filter_expression"):
            release["filters"].append({
                "id": f"FILTER_{key}", "source_object_id": key,
                "expression": obj["quality_filter_expression"],
                "reason": "EXPLICIT_SOURCE_FILTER", "ordinal": 1,
            })
        if obj.get("processing_route") == "REFERENCE_DATA":
            release["reference_entities"].append({
                "id": f"REF_{key}", "target": target("processing", "business_vault", obj["reference_table"]),
                "keys": keys,
                "hash_columns": [r["target_column"] for r in qcols if r["target_column"] not in keys],
                "source_object_id": key, "schema_id": schema_id, "delete_policy": delete(obj),
            })
    for col in quality:
        release["mappings"].append({
            "id": col["mapping_id"], "source_object_id": col["source_object_id"],
            "entity_id": node("source_objects", col["source_object_id"]),
            "target_column": col["target_column"], "ordinal": col["ordinal_position"],
            "data_type": col["target_data_type"].upper().replace("BIGINT", "LONG"),
            "expression": _quality_mapping_expression(col), "nullable": col["is_nullable"],
            "role": "BUSINESS_KEY" if col["is_business_key"] else "DESCRIPTIVE",
            "hashdiff": False, "cast_policy": "REJECT_INVALID",
        })
    dv_nodes = {r["dv_entity_id"]: node(
        "business_entities" if r["dv_zone"] == "BUSINESS_VAULT" else "vault_entities", r["dv_entity_id"],
    ) for r in dv}
    for entity in dv:
        key = entity["dv_entity_id"]
        cols = [r for r in dv_map if r["dv_entity_id"] == key]
        keys = entity["business_key_columns"] or [entity["hash_key_column"]]
        schema_id = schema(f"SCHEMA_{key}", cols, keys)
        common = dict(
            id=key, type=entity["dv_entity_type"],
            target=target("processing", entity["target_schema"], entity["target_table"]),
            keys=keys, parents=[dv_nodes[p] for p in entity["parent_entity_ids"] or []],
            schema_id=schema_id,
        )
        if entity["dv_zone"] == "BUSINESS_VAULT":
            common.update(expression_ref=None, temporal_policy="COMMITTED_AS_OF")
            release["business_entities"].append(common)
        else:
            common.update(
                hash_columns=[r["target_column"] for r in sorted(cols, key=lambda c: c["ordinal_position"]) if r["is_in_hashdiff"]],
                source_objects=sorted({r["source_object_id"] for r in cols if r["source_object_id"] in object_ids}),
            )
            release["vault_entities"].append(common)
        for col in cols:
            if col["source_object_id"] in object_ids or entity["dv_zone"] == "BUSINESS_VAULT":
                release["mappings"].append({
                    "id": col["dv_mapping_id"],
                    "source_object_id": col["source_object_id"] if col["source_object_id"] in object_ids else None,
                    "entity_id": dv_nodes[key], "target_column": col["target_column"],
                    "ordinal": col["ordinal_position"],
                    "data_type": col["target_data_type"].upper().replace("BIGINT", "LONG"),
                    "expression": col["source_expression"], "nullable": True,
                    "role": col["column_role"], "hashdiff": col["is_in_hashdiff"],
                    "cast_policy": "REJECT_INVALID",
                })
    for rule in read("meta_quality_rule"):
        if rule["source_object_id"] not in object_ids:
            continue
        release["quality_rules"].append({
            "id": rule["rule_id"], "source_object_id": rule["source_object_id"],
            "type": rule["rule_type"], "columns": rule["target_columns"],
            "expression": rule["rule_expression"], "severity": rule["severity"],
            "action": "ANNOTATE" if rule["severity"] == "WARNING" else ("BLOCK" if rule["is_blocking"] else "REJECT"),
            "code": rule["reject_reason_code"], "threshold_pct": rule["threshold_pct"],
            "denominator": "POST_FILTER", "ordinal": rule["execution_order"],
        })
    products = [p for p in read("meta_gold_data_product") if p["publication_group_id"] != "SHAREPOINT_SALES_INPUT"]
    groups = {p["publication_group_id"] for p in products}
    group_for_source = {
        "SALES": "SALES_MART", "CBS": "CBS_JEUGDZORG_MART", "ECB": "ECB_REFERENCE_MART",
        "NAGER": "NAGER_REFERENCE_MART", "FABRIC_SALES": "FABRIC_SALES_MART",
    }
    for entity in gold:
        key = entity["gold_entity_id"]
        group = entity.get("publication_group_id") or group_for_source[entity.get("source_system_id", "SALES")]
        if group not in groups:
            raise ContractError(f"Missing publication group: {group}")
        table_name = entity["target_table"] + ("_v1" if entity["gold_layer"] == "CURRENT" else "")
        table_columns = table_contracts[
            "historical" if entity["gold_layer"] == "HISTORICAL" else "current"
        ].get(table_name)
        if table_columns is None:
            raise ContractError(f"Missing legacy Gold schema: {key}")
        release["gold_entities"].append({
            "id": key, "layer": entity["gold_layer"],
            "target": target("gold", "historical" if entity["gold_layer"] == "HISTORICAL" else "release_data", entity["target_table"]),
            "keys": entity["business_key_columns"],
            "source_system_id": entity.get("source_system_id", "SALES"),
            "product_id": group, "expression_ref": expression(key, entity["select_sql"]),
            "schema_id": schema(f"SCHEMA_{key}", [], entity["business_key_columns"],
                                table_columns=table_columns),
            "temporal_policy": "COMMITTED_AS_OF",
        })
    for product in products:
        key = product["publication_group_id"]
        release["products"].append({
            "id": key, "entities": [r["id"] for r in release["gold_entities"] if r["product_id"] == key],
            "consumer_mode": "DIRECTQUERY_PINNED", "compatibility_policy": product["compatibility_policy"],
            "refresh_hours": product["refresh_sla_hours"],
        })
    edges: set[tuple[str, str]] = set()
    gold_ids = {r["gold_entity_id"] for r in gold}
    all_sources = {r["source_system_id"] for r in objects}
    for dependency in read("meta_dependency"):
        if dependency["dependency_type"] == "DELIVERY_COMPLETE" and dependency["entity_layer"] == "QUALITY":
            # Bronze/Quality readiness remains the mandatory manifest gate;
            # collapsing both phases into a source node would create a cycle.
            continue
        def resolve(key: str) -> list[str]:
            if key in dv_nodes:
                return [dv_nodes[key]]
            if key in gold_ids:
                return [node("gold_entities", key)]
            if key in object_ids:
                return [node("source_objects", key)]
            if key in all_sources:
                return [node("source_objects", obj["source_object_id"]) for obj in objects if obj["source_system_id"] == key]
            if key.startswith("SHAREPOINT") or key in {"GH_DIM_PRICE_AGREEMENT", "GH_FCT_SALES_BUDGET", "GC_DIM_PRICE_AGREEMENT", "GC_FCT_SALES_BUDGET"}:
                return []
            raise ContractError(f"Unknown legacy dependency: {key}")
        for entity_node in resolve(dependency["entity_id"]):
            for parent in resolve(dependency["depends_on_entity_id"]):
                if entity_node != parent:
                    edges.add((entity_node, parent))
    for obj in release["reference_entities"]:
        edges.add((node("reference_entities", obj["id"]), node("source_objects", obj["source_object_id"])))
    for entity in release["gold_entities"]:
        if entity["layer"] == "HISTORICAL":
            for reference in release["reference_entities"]:
                source_system = reference["source_object_id"].split(".", 1)[0]
                if entity["source_system_id"] == source_system:
                    edges.add((node("gold_entities", entity["id"]), node("reference_entities", reference["id"])))
    for index, (entity_node, parent) in enumerate(sorted(edges), 1):
        release["dependencies"].append({"id": f"DEP_{index:03}", "entity": entity_node, "depends_on": parent})
    generated_files["fixtures/scope.json"] = json.dumps({
        "source_systems": sorted(all_sources), "source_objects": sorted(object_ids),
        "excluded_sources": ["SHAREPOINT"], "evidence_kind": "CONTRACT_FIXTURE_NOT_SOURCE_DATA",
    }, indent=2) + "\n"
    output.mkdir(parents=True, exist_ok=True)
    for relative, text in generated_files.items():
        path = output.joinpath(*relative.split("/"))
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists() and path.read_text(encoding="utf-8") != text:
            raise ContractError(f"Conversion would overwrite changed project file: {relative}")
        path.write_text(text, encoding="utf-8", newline="\n")
        release["files"][relative] = hashlib.sha256(path.read_bytes()).hexdigest()
    release["release_id"] = fingerprint(release)
    ProjectRelease.validate(release, output)
    destination = output / "release.json"
    text = json.dumps(release, ensure_ascii=False, indent=2) + "\n"
    if destination.exists() and destination.read_text(encoding="utf-8") != text:
        raise ContractError("Conversion would overwrite an existing changed release")
    destination.write_text(text, encoding="utf-8", newline="\n")
    return release
