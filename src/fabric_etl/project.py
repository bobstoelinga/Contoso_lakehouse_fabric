"""Versioned project metadata validation, independent of Spark."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from fabric_etl import __version__
from fabric_etl.contracts import ContractError, fields, fingerprint, identifier, load_json


RECORD_FIELDS = {
    "source_systems": "id name delivery_mode owner policy_id",
    "source_objects": (
        "id source_system_id connector_id schema_id keys route format load_strategy "
        "delete_policy mandatory freshness_hours targets drift_policy"
    ),
    "connectors": "id type endpoint connection_ref parameters timeout_seconds max_retries",
    "schemas": "id scope columns status",
    "mappings": (
        "id source_object_id entity_id target_column ordinal data_type expression "
        "nullable role hashdiff cast_policy"
    ),
    "filters": "id source_object_id expression reason ordinal",
    "quality_rules": (
        "id source_object_id type columns expression severity action code "
        "threshold_pct denominator ordinal"
    ),
    "dependencies": "id entity depends_on",
    "vault_entities": "id type target keys hash_columns parents source_objects schema_id",
    "reference_entities": "id target keys hash_columns source_object_id schema_id delete_policy",
    "business_entities": "id type target keys parents expression_ref schema_id temporal_policy",
    "gold_entities": "id layer target keys source_system_id product_id expression_ref schema_id temporal_policy",
    "products": "id entities consumer_mode compatibility_policy refresh_hours",
    "policies": "id classification owner retention_days status",
    "expressions": "id engine path bindings review_status",
}
COLLECTIONS = set(RECORD_FIELDS)
ENTITY_COLLECTIONS = (
    "source_objects", "vault_entities", "reference_entities",
    "business_entities", "gold_entities",
)
ENUMS = {
    ("source_systems", "delivery_mode"): {"DAILY", "ON_DEMAND", "YEARLY"},
    ("source_objects", "route"): {"RAW_VAULT", "REFERENCE_DATA"},
    ("source_objects", "format"): {"PARQUET", "JSON", "CSV"},
    ("source_objects", "load_strategy"): {"SNAPSHOT_SCD2", "INCREMENTAL_APPEND"},
    ("source_objects", "drift_policy"): {"STRICT", "RESCUE", "ALLOW_NEW_COLUMNS_WITH_APPROVAL"},
    ("connectors", "type"): {"FILE_MANIFEST", "HTTP_JSON", "HTTP_CSV", "JDBC"},
    ("schemas", "scope"): {"QUALITY", "ENTITY"},
    ("schemas", "status"): {"DECLARED", "REQUIRES_COMPILE"},
    ("mappings", "role"): {"BUSINESS_KEY", "DRIVING_KEY", "DEGENERATE", "DESCRIPTIVE"},
    ("mappings", "cast_policy"): {"REJECT_INVALID"},
    ("quality_rules", "type"): {"NOT_NULL", "UNIQUE", "REGEX", "RANGE", "ALLOWED_VALUES", "REFERENTIAL", "CUSTOM_SQL"},
    ("quality_rules", "severity"): {"ERROR", "WARNING"},
    ("quality_rules", "action"): {"REJECT", "BLOCK", "ANNOTATE"},
    ("quality_rules", "denominator"): {"POST_FILTER"},
    ("vault_entities", "type"): {"HUB", "LINK", "SATELLITE", "LINK_SATELLITE"},
    ("business_entities", "type"): {"SATELLITE", "PIT"},
    ("business_entities", "temporal_policy"): {"COMMITTED_AS_OF"},
    ("gold_entities", "layer"): {"HISTORICAL", "CURRENT"},
    ("gold_entities", "temporal_policy"): {"COMMITTED_AS_OF"},
    ("products", "consumer_mode"): {"DIRECTQUERY_PINNED"},
    ("products", "compatibility_policy"): {"BACKWARD_COMPATIBLE", "VERSIONED_BREAKING_CHANGE"},
    ("policies", "classification"): {"PUBLIC", "INTERNAL", "CONFIDENTIAL", "RESTRICTED"},
    ("policies", "status"): {"DEVELOPMENT", "APPROVED"},
    ("expressions", "engine"): {"SPARK_SQL"},
    ("expressions", "review_status"): {"REQUIRES_REVIEW", "APPROVED"},
}
LIST_FIELDS = {
    "keys", "hash_columns", "parents", "source_objects", "columns", "entities", "bindings",
}
OPTIONAL_FIELDS = {
    ("connectors", "endpoint"), ("connectors", "connection_ref"),
    ("business_entities", "expression_ref"), ("mappings", "source_object_id"),
}
REFERENCE_FIELDS = {
    "policy_id": "policies", "connector_id": "connectors", "schema_id": "schemas",
    "source_object_id": "source_objects", "source_system_id": "source_systems",
    "product_id": "products", "expression_ref": "expressions",
}


def data_type(value: object) -> str:
    if not isinstance(value, str):
        raise ContractError("data_type must be a string")
    if value in {"STRING", "BOOLEAN", "BYTE", "SHORT", "INT", "LONG", "DATE", "TIMESTAMP", "DOUBLE", "FLOAT", "BINARY"}:
        return value
    match = re.fullmatch(r"DECIMAL\((\d+),(\d+)\)", value)
    if match and 1 <= int(match[1]) <= 38 and 0 <= int(match[2]) <= int(match[1]):
        return value
    raise ContractError(f"Unsupported scalar datatype: {value}")


def safe_file(root: Path, relative: object) -> Path:
    if not isinstance(relative, str) or not re.fullmatch(r"[A-Za-z0-9_.-]+(?:/[A-Za-z0-9_.-]+)*", relative):
        raise ContractError(f"Unsafe project file: {relative!r}")
    if any(segment in {".", ".."} for segment in relative.split("/")):
        raise ContractError(f"Unsafe project file: {relative}")
    path = root.joinpath(*relative.split("/")).resolve(strict=True)
    if not path.is_relative_to(root.resolve(strict=True)) or not path.is_file():
        raise ContractError(f"Project file escapes root: {relative}")
    return path


def _text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ContractError(f"{label} must be a non-empty string")
    return value


def _strings(value: object, label: str, *, allow_empty: bool = True) -> list[str]:
    if not isinstance(value, list) or any(not isinstance(v, str) or not v.strip() for v in value):
        raise ContractError(f"{label} must be a list of strings")
    if len(set(value)) != len(value) or (not allow_empty and not value):
        raise ContractError(f"{label} must contain unique entries and may not be empty")
    return value


def _number(value: object, label: str, low: float, high: float = float("inf")) -> None:
    if type(value) not in {int, float} or not low <= value <= high:
        raise ContractError(f"{label} outside permitted range")


def _target(value: object) -> tuple[str, str, str]:
    target = fields(value, {"role", "schema", "table"}, "Target")
    if not isinstance(target["role"], str) or target["role"] not in {"control", "processing", "gold"}:
        raise ContractError("Unsupported target role")
    return str(target["role"]), identifier(target["schema"]), identifier(target["table"])


def _delete(value: object) -> None:
    policy = fields(value, {"mode", "flag_column", "absence", "max_delete_pct"}, "Delete policy")
    if not isinstance(policy["mode"], str) or policy["mode"] not in {"NONE", "SOFT_DELETE_FLAG"} or type(policy["absence"]) is not bool:
        raise ContractError("Unsupported delete policy")
    if policy["mode"] == "SOFT_DELETE_FLAG":
        identifier(_text(policy["flag_column"], "flag_column"))
    elif policy["flag_column"] is not None or policy["absence"]:
        raise ContractError("NONE delete policy may not contain a flag or absence deletes")
    if policy["max_delete_pct"] is not None:
        _number(policy["max_delete_pct"], "max_delete_pct", 0, 100)


def node(collection: str, entity_id: str) -> str:
    return f"{collection}:{entity_id}"


@dataclass(frozen=True)
class ProjectRelease:
    document: dict[str, Any]
    topological_order: tuple[str, ...]
    runtime_blockers: tuple[str, ...]

    @property
    def release_id(self) -> str:
        return self.document["release_id"]

    @classmethod
    def load(cls, path: Path, *, require_runtime: bool = False) -> ProjectRelease:
        return cls.validate(load_json(path), path.parent, require_runtime=require_runtime)

    @classmethod
    def validate(
        cls, value: object, root: Path, *, require_runtime: bool = False,
    ) -> ProjectRelease:
        envelope = fields(value, COLLECTIONS | {
            "project_id", "contract_version", "framework_version", "hash_contract_version",
            "schema_contract_version", "release_id", "files",
        }, "Project release")
        identifier(envelope["project_id"])
        if (
            envelope["contract_version"] != "1.0"
            or envelope["framework_version"] != __version__
            or envelope["schema_contract_version"] != "1.0"
            or envelope["hash_contract_version"] != "fabric-dv-key-v1"
        ):
            raise ContractError("Unsupported project/framework/schema/hash version")
        content = {key: val for key, val in envelope.items() if key != "release_id"}
        if envelope["release_id"] != fingerprint(content):
            raise ContractError("Project release fingerprint mismatch")
        file_manifest = envelope["files"]
        if not isinstance(file_manifest, dict):
            raise ContractError("files must map paths to SHA-256 fingerprints")
        for relative, digest in file_manifest.items():
            if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest):
                raise ContractError("Invalid file fingerprint")
            if hashlib.sha256(safe_file(root, relative).read_bytes()).hexdigest() != digest:
                raise ContractError(f"Project file fingerprint mismatch: {relative}")

        indices: dict[str, dict[str, dict[str, Any]]] = {}
        blockers: list[str] = []
        targets: set[tuple[str, str, str]] = set()
        for collection, field_names in RECORD_FIELDS.items():
            raw = envelope[collection]
            if not isinstance(raw, list):
                raise ContractError(f"{collection} must be an array")
            indices[collection] = {}
            for candidate in raw:
                record = fields(candidate, set(field_names.split()), collection)
                key = _text(record["id"], f"{collection}.id")
                if key in indices[collection]:
                    raise ContractError(f"Duplicate {collection} key: {key}")
                indices[collection][key] = record
                for (enum_collection, enum_field), allowed in ENUMS.items():
                    if collection == enum_collection:
                        enum_value = record[enum_field]
                        if not isinstance(enum_value, str) or enum_value not in allowed:
                            raise ContractError(f"Unsupported {collection}.{enum_field}: {enum_value!r}")
                for field, field_value in record.items():
                    if (collection, field) in ENUMS:
                        if not isinstance(field_value, str) or field_value not in ENUMS[collection, field]:
                            raise ContractError(f"Unsupported {collection}.{field}: {field_value!r}")
                    elif field in {"mandatory", "nullable", "hashdiff"}:
                        if type(field_value) is not bool:
                            raise ContractError(f"{collection}.{field} must be boolean")
                    elif field in {"ordinal", "timeout_seconds", "max_retries"}:
                        if type(field_value) is not int or field_value < (0 if field == "max_retries" else 1):
                            raise ContractError(f"Invalid integer {collection}.{field}")
                    elif field in {"freshness_hours", "refresh_hours"}:
                        _number(field_value, field, 0.001)
                    elif field == "threshold_pct":
                        _number(field_value, field, 0, 100)
                    elif field == "retention_days":
                        if field_value is not None and (type(field_value) is not int or field_value < 1):
                            raise ContractError("Invalid retention_days")
                    elif field == "delete_policy":
                        _delete(field_value)
                    elif field == "target":
                        target = _target(field_value)
                        if target in targets:
                            raise ContractError(f"Duplicate physical target: {target}")
                        targets.add(target)
                    elif field == "targets":
                        table_targets = fields(field_value, {"bronze", "quality", "reject"}, "Source targets")
                        for target_value in table_targets.values():
                            target = _target(target_value)
                            if target in targets:
                                raise ContractError(f"Duplicate physical target: {target}")
                            targets.add(target)
                    elif field == "columns" and collection == "schemas":
                        cls._columns(field_value)
                    elif field == "parameters":
                        cls._parameters(record)
                    elif field in LIST_FIELDS:
                        _strings(field_value, f"{collection}.{field}")
                    elif (collection, field) in OPTIONAL_FIELDS and field_value is None:
                        pass
                    else:
                        _text(field_value, f"{collection}.{field}")
                if collection == "schemas" and record["status"] != "DECLARED":
                    blockers.append(f"Schema compile required: {key}")
                if collection == "expressions":
                    if record["path"] not in file_manifest:
                        raise ContractError(f"Expression not fingerprinted: {key}")
                    sql = safe_file(root, record["path"]).read_text(encoding="utf-8")
                    placeholders = set(re.findall(r"\{([A-Za-z_][A-Za-z0-9_]*)\}", sql))
                    if placeholders != set(record["bindings"]) or placeholders - {"processing", "gold"}:
                        raise ContractError(f"Unregistered SQL bindings: {key}")
                    if re.search(r"\$\{|/Volumes/|contoso_(?:vault|gold|bronze)_", sql):
                        raise ContractError(f"Unresolved legacy binding in expression: {key}")
                    if record["review_status"] != "APPROVED":
                        blockers.append(f"Expression review required: {key}")

        for required in ("source_systems", "source_objects", "connectors", "schemas", "products", "gold_entities", "policies"):
            if not indices[required]:
                raise ContractError(f"Required collection is empty: {required}")
        entities = {
            node(collection, key): record
            for collection in ENTITY_COLLECTIONS for key, record in indices[collection].items()
        }
        for collection, records in indices.items():
            for key, record in records.items():
                for field, referenced_collection in REFERENCE_FIELDS.items():
                    if field in record and record[field] is not None:
                        if record[field] not in indices[referenced_collection]:
                            raise ContractError(f"Dangling {collection}.{field}: {key}")
                if collection == "source_systems" and not any(
                    obj["source_system_id"] == key for obj in indices["source_objects"].values()
                ):
                    raise ContractError(f"Source system has no objects: {key}")
                if "keys" in record:
                    _strings(record["keys"], f"{key}.keys", allow_empty=False)
                if "schema_id" in record:
                    schema = indices["schemas"][record["schema_id"]]
                    names = {col["name"] for col in schema["columns"]}
                    if set(record.get("keys", [])) - names:
                        raise ContractError(f"Key missing from declared schema: {key}")
                    if set(record.get("hash_columns", [])) - names:
                        raise ContractError(f"Hash column missing from declared schema: {key}")
                if "source_object_id" in record and record["source_object_id"] is None:
                    if collection != "mappings" or not record["entity_id"].startswith("business_entities:"):
                        raise ContractError("Only Business Vault mappings may omit source_object_id")
                if collection == "source_objects":
                    if not record["mandatory"]:
                        raise ContractError("Optional delivery objects are not supported in contract 1.0")
                    if record["delete_policy"]["absence"] and record["delete_policy"]["max_delete_pct"] is None:
                        blockers.append(f"Absence-delete approval required: {key}")
                    if record["route"] == "REFERENCE_DATA" and not any(
                        r["source_object_id"] == key for r in indices["reference_entities"].values()
                    ):
                        raise ContractError(f"Reference route missing entity: {key}")
                    if record["route"] == "RAW_VAULT" and not any(
                        key in r["source_objects"] for r in indices["vault_entities"].values()
                    ):
                        raise ContractError(f"Vault route missing entity: {key}")
                if collection in {"vault_entities", "business_entities"}:
                    for parent in record["parents"]:
                        if parent not in entities:
                            raise ContractError(f"Unknown parent: {parent}")
                if collection == "vault_entities":
                    for source in record["source_objects"]:
                        if source not in indices["source_objects"]:
                            raise ContractError(f"Unknown Vault source: {source}")
                if collection == "products":
                    _strings(record["entities"], "product entities", allow_empty=False)
                    for entity_id in record["entities"]:
                        if entity_id not in indices["gold_entities"]:
                            raise ContractError(f"Unknown product entity: {entity_id}")
                        if indices["gold_entities"][entity_id]["product_id"] != key:
                            raise ContractError(f"Product membership mismatch: {entity_id}")
                if collection == "gold_entities":
                    if record["product_id"] is None or key not in indices["products"][record["product_id"]]["entities"]:
                        raise ContractError(f"Gold product membership missing: {key}")
                if collection == "business_entities" and record["expression_ref"] is None:
                    blockers.append(f"Business Vault implementation required: {key}")
                if collection == "policies":
                    if record["status"] != "APPROVED" or record["retention_days"] is None:
                        blockers.append(f"Governance approval required: {key}")
                if collection == "connectors":
                    if record["type"] == "JDBC":
                        query_ref = record["parameters"]["source_query_ref"]
                        if query_ref not in indices["expressions"]:
                            raise ContractError(f"Missing JDBC query expression: {key}")
                    if record["connection_ref"] is not None:
                        blockers.append(f"Connection binding verification required: {key}")
        cls._validate_mappings(indices, entities)
        order = cls._dag(indices, entities)
        if require_runtime and blockers:
            raise ContractError("Runtime release blocked: " + "; ".join(blockers))
        return cls(envelope, order, tuple(blockers))

    @staticmethod
    def _columns(value: object) -> None:
        if not isinstance(value, list) or not value:
            raise ContractError("Schema columns must be a non-empty array")
        names: set[str] = set()
        ordinals: set[int] = set()
        for raw in value:
            col = fields(raw, {"name", "data_type", "nullable", "ordinal", "source_name"}, "Schema column")
            name = identifier(col["name"])
            data_type(col["data_type"])
            identifier(col["source_name"])
            if type(col["nullable"]) is not bool or type(col["ordinal"]) is not int or col["ordinal"] < 1:
                raise ContractError("Invalid schema column nullability/ordinal")
            if name.casefold() in names or col["ordinal"] in ordinals:
                raise ContractError("Duplicate schema column or ordinal")
            names.add(name.casefold())
            ordinals.add(col["ordinal"])
        if ordinals != set(range(1, len(value) + 1)):
            raise ContractError("Schema ordinals must be contiguous from 1")

    @staticmethod
    def _parameters(record: dict[str, Any]) -> None:
        expected = {
            "FILE_MANIFEST": {"pattern"},
            "HTTP_JSON": {"records_key", "next_link_key"},
            "HTTP_CSV": {"encoding", "delimiter"},
            "JDBC": {"source_query_ref"},
        }[record["type"]]
        params = fields(record["parameters"], expected, "Connector parameters")
        for key, val in params.items():
            if val is not None and not isinstance(val, str):
                raise ContractError(f"Invalid connector parameter: {key}")
            if key in {"pattern", "encoding", "delimiter", "source_query_ref"}:
                _text(val, f"Connector parameter {key}")
        if record["type"].startswith("HTTP"):
            endpoint = urlparse(_text(record["endpoint"], "HTTP endpoint"))
            if endpoint.scheme != "https" or not endpoint.hostname or endpoint.username or endpoint.password:
                raise ContractError("HTTP connector requires credential-free HTTPS endpoint")
            if endpoint.query and re.search(r"(?:token|secret|password|key)=", endpoint.query, re.I):
                raise ContractError("Credentials not allowed in endpoint query")
        elif record["endpoint"] is not None:
            raise ContractError("FILE/JDBC endpoint must be supplied through a binding, not project metadata")
        if record["type"] == "JDBC" and not record["connection_ref"]:
            raise ContractError("JDBC requires a connection reference")

    @staticmethod
    def _validate_mappings(indices: dict, entities: dict) -> None:
        positions: set[tuple[str, str, int]] = set()
        columns: set[tuple[str, str, str]] = set()
        for record in indices["mappings"].values():
            data_type(record["data_type"])
            identifier(record["target_column"])
            if record["entity_id"] not in entities:
                raise ContractError(f"Unknown mapping entity: {record['entity_id']}")
            target_schema = indices["schemas"][entities[record["entity_id"]]["schema_id"]]
            target_names = {column["name"] for column in target_schema["columns"]}
            if record["target_column"] not in target_names:
                raise ContractError(f"Mapping column missing from entity schema: {record['id']}")
            group = record["source_object_id"], record["entity_id"]
            position = (*group, record["ordinal"])
            column = (*group, record["target_column"].casefold())
            if position in positions or column in columns:
                raise ContractError("Duplicate mapping target column or ordinal")
            positions.add(position)
            columns.add(column)
        for key in indices["source_objects"]:
            if not any(r["source_object_id"] == key and r["entity_id"] == node("source_objects", key)
                       for r in indices["mappings"].values()):
                raise ContractError(f"Missing Quality mapping: {key}")
        for record in indices["quality_rules"].values():
            source = indices["source_objects"][record["source_object_id"]]
            names = {col["name"] for col in indices["schemas"][source["schema_id"]]["columns"]}
            if set(record["columns"]) - names:
                raise ContractError(f"DQ column absent from Quality schema: {record['id']}")
            if record["severity"] == "WARNING" and record["action"] != "ANNOTATE":
                raise ContractError("WARNING rule must annotate, not remove records")

    @staticmethod
    def _dag(indices: dict, entities: dict) -> tuple[str, ...]:
        graph: dict[str, set[str]] = {key: set() for key in entities}
        for key, record in entities.items():
            graph[key].update(record.get("parents", []))
        seen: set[tuple[str, str]] = set()
        for dependency in indices["dependencies"].values():
            edge = dependency["entity"], dependency["depends_on"]
            if edge[0] not in graph or edge[1] not in graph or edge in seen:
                raise ContractError(f"Unknown or duplicate dependency: {edge}")
            seen.add(edge)
            graph[edge[0]].add(edge[1])
        result: list[str] = []
        while graph:
            ready = sorted(key for key, parents in graph.items() if not parents)
            if not ready:
                raise ContractError("Cyclic metadata dependencies")
            result.extend(ready)
            for key in ready:
                del graph[key]
            for parents in graph.values():
                parents.difference_update(ready)
        return tuple(result)
