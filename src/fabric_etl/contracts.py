"""Strict configuration, identifier and hashing contracts."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable
from uuid import UUID


class ContractError(ValueError):
    """Input violates a versioned template contract."""


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ContractError(f"Duplicate JSON field: {key}")
        result[key] = value
    return result


def _reject_constant(value: str) -> object:
    raise ContractError(f"Non-finite JSON number: {value}")


def load_json(path: Path) -> object:
    return parse_json(path.read_text(encoding="utf-8-sig"))


def parse_json(text: str) -> object:
    return json.loads(
        text,
        object_pairs_hook=_unique_object,
        parse_constant=_reject_constant,
    )


def canonical_json(value: object) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def fingerprint(value: object) -> str:
    return hashlib.sha256(canonical_json(value)).hexdigest()


def identifier(value: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ContractError(f"Unsupported identifier: {value!r}")
    return value


def required_text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ContractError(f"{field} must be a non-empty string")
    return value


def fields(value: object, expected: set[str], label: str) -> dict[str, object]:
    if not isinstance(value, dict) or set(value) != expected:
        raise ContractError(f"{label} requires exactly these fields: {sorted(expected)}")
    return value


@dataclass(frozen=True)
class LakehouseBinding:
    name: str
    item_id: str | None


@dataclass(frozen=True)
class Environment:
    project_id: str
    environment: str
    tenant_id: str
    workspace_id: str
    workspace_name: str
    lakehouses: dict[str, LakehouseBinding]

    @classmethod
    def from_dict(cls, value: object) -> Environment:
        data = fields(value, {
            "contract_version", "project_id", "environment", "workspace_id",
            "workspace_name", "lakehouses", "tenant_id",
        }, "Environment")
        if data["contract_version"] != "1.1":
            raise ContractError("Unsupported environment contract version")
        tenant_id = str(UUID(required_text(data["tenant_id"], "tenant_id")))
        workspace_id = str(UUID(required_text(data["workspace_id"], "workspace_id")))
        roles = fields(data["lakehouses"], {"control", "processing", "gold"}, "Lakehouses")
        bindings: dict[str, LakehouseBinding] = {}
        for role, raw in roles.items():
            binding = fields(raw, {"name", "item_id"}, f"Lakehouse {role}")
            name = identifier(required_text(binding["name"], "lakehouse name"))
            item_id = binding["item_id"]
            if item_id is not None:
                item_id = str(UUID(required_text(item_id, "item_id")))
            bindings[role] = LakehouseBinding(name, item_id)
        names = [binding.name.casefold() for binding in bindings.values()]
        ids = [binding.item_id for binding in bindings.values() if binding.item_id is not None]
        if len(set(names)) != 3 or len(set(ids)) != len(ids):
            raise ContractError("Lakehouse roles must use distinct names and item IDs")
        return cls(
            identifier(required_text(data["project_id"], "project_id")),
            identifier(required_text(data["environment"], "environment")),
            tenant_id,
            workspace_id,
            identifier(required_text(data["workspace_name"], "workspace_name")),
            bindings,
        )

    def spark_table(self, role: str, schema: str, table: str) -> str:
        if role not in self.lakehouses:
            raise ContractError(f"Unknown lakehouse role: {role}")
        segments = (
            self.workspace_name, self.lakehouses[role].name,
            identifier(schema), identifier(table),
        )
        return ".".join(f"`{segment}`" for segment in segments)

    @staticmethod
    def sql_table(schema: str, table: str) -> str:
        return f"[{identifier(schema)}].[{identifier(table)}]"

    def require_provisioned(self) -> None:
        missing = [role for role, binding in self.lakehouses.items() if binding.item_id is None]
        if missing:
            raise ContractError(f"Unprovisioned lakehouse bindings: {', '.join(missing)}")


def hash_key(namespace: str, values: Iterable[str | None]) -> str:
    """Hash v1: ordered normalized strings, preserving null and empty."""
    namespace = required_text(namespace, "hash namespace")
    normalized: list[str | None] = []
    for value in values:
        if value is not None and not isinstance(value, str):
            raise ContractError("Hash inputs require explicitly canonicalized strings or null")
        normalized.append(None if value is None else value.strip().upper())
    if not normalized:
        raise ContractError("At least one hash column is required")
    return fingerprint(["fabric-dv-key-v1", namespace, normalized])
