"""Validated local deployment plans; no API calls or success-shaped runtime fallback."""

from __future__ import annotations

import ast
import base64
import hashlib
import json
import re
import zipfile
from email.parser import BytesParser
from pathlib import Path
from typing import Any

from fabric_etl import __version__
from fabric_etl.contracts import ContractError, Environment, fields, fingerprint, load_json
from fabric_etl.project import ProjectRelease, safe_file


def build_plan(repository: Path, project_path: Path, environment_path: Path, wheel: Path) -> dict[str, Any]:
    project = ProjectRelease.load(project_path)
    environment_value = load_json(environment_path)
    environment = Environment.from_dict(environment_value)
    if project.document["project_id"] != environment.project_id:
        raise ContractError("Project and environment identities differ")
    manifest = fields(load_json(repository / "fabric" / "deployment.json"), {
        "contract_version", "scope", "framework_version", "items", "migrations",
        "pending_runtime_stages",
    }, "Deployment manifest")
    if (
        manifest["contract_version"] != "1.0"
        or manifest["framework_version"] != __version__
        or manifest["scope"] != "PHASE3_FOUNDATION_PREPARATION"
    ):
        raise ContractError("Unsupported deployment manifest")
    with zipfile.ZipFile(wheel) as archive:
        names = archive.namelist()
        metadata_paths = [n for n in names if n.endswith(".dist-info/METADATA")]
        if len(metadata_paths) != 1 or any(n.startswith("contoso_lakehouse/") for n in names):
            raise ContractError("Wheel must contain only the standalone Fabric distribution")
        metadata = BytesParser().parsebytes(archive.read(metadata_paths[0]))
        if metadata["Name"] != "fabric-etl" or metadata["Version"] != __version__:
            raise ContractError("Wheel identity/version does not match deployment")
        required = {"fabric_etl/__init__.py", "fabric_etl/runtime.py", "fabric_etl/project.py"}
        if not required.issubset(names):
            raise ContractError("Wheel is missing required runtime modules")
        expected = {
            "fabric_etl/" + source.name
            for source in (repository / "src" / "fabric_etl").glob("*.py")
        }
        actual = {name for name in names if name.endswith(".py")}
        if actual != expected:
            raise ContractError("Wheel module inventory differs from checked source")
        for name in names:
            if name.startswith("fabric_etl/") and name.endswith(".py"):
                source = safe_file(repository, "src/" + name)
                if archive.read(name) != source.read_bytes():
                    raise ContractError(f"Wheel code differs from checked source: {name}")
    wheel_hash = hashlib.sha256(wheel.read_bytes()).hexdigest()
    items = manifest["items"]
    if not isinstance(items, list) or not items:
        raise ContractError("Deployment requires item definitions")
    definitions = []
    known: set[str] = set()
    item_fingerprints: dict[str, str] = {}
    for raw in items:
        item = fields(raw, {"id", "type", "display_name", "format", "parts", "requires"}, "Item")
        if not isinstance(item["id"], str) or item["id"] in known:
            raise ContractError("Invalid or duplicate deployment item ID")
        if not isinstance(item["requires"], list) or set(item["requires"]) - known:
            raise ContractError("Item prerequisites must precede their dependent")
        if item["type"] not in {"Environment", "Notebook"}:
            raise ContractError("Unsupported phase-three item type")
        parts = item["parts"]
        if not isinstance(parts, list) or not parts:
            raise ContractError("Definition parts must be non-empty")
        seen: set[str] = set()
        payloads = []
        for raw_part in parts:
            part = fields(raw_part, {"path", "source"}, "Definition part")
            if part["path"] in seen:
                raise ContractError("Duplicate definition part")
            seen.add(part["path"])
            data = safe_file(repository, part["source"]).read_bytes()
            if item["type"] == "Notebook":
                if part["path"] != "notebook-content.py" or item["format"] != "fabricGitSource":
                    raise ContractError("Unsupported Notebook definition format")
                ast.parse(data.decode("utf-8"), filename=part["source"])
                if not data.startswith(b"# Fabric notebook source"):
                    raise ContractError("Notebook is not Fabric Git source")
                # Metadata injection is scoped to this declared source header.
                header = '"dependencies": {}'
                if data.decode("utf-8").count(header) != 1:
                    raise ContractError("Notebook must declare exactly one empty dependency header")
                dependency = {
                    "environment": {"environmentId": "${etl_environment_item_id}",
                                    "workspaceId": environment.workspace_id},
                }
                data = data.decode("utf-8").replace(
                    header, '"dependencies": ' + json.dumps(dependency), 1,
                ).encode("utf-8")
            elif part["path"] != "Setting/Sparkcompute.yml" or item["format"] is not None:
                raise ContractError("Unsupported Environment definition part")
            payloads.append({
                "path": part["path"], "payloadType": "InlineBase64",
                "payload": base64.b64encode(data).decode("ascii"),
            })
        if item["type"] == "Environment":
            payloads.append({
                "path": f"Libraries/CustomLibraries/{wheel.name}",
                "payloadType": "InlineBase64",
                "payload": base64.b64encode(wheel.read_bytes()).decode("ascii"),
            })
        definition = {"parts": payloads}
        if item["format"] is not None:
            definition["format"] = item["format"]
        display_name = item["display_name"].format(
            project_id=environment.project_id, environment=environment.environment,
        )
        item_fingerprints[item["id"]] = fingerprint(definition)
        definitions.append({
            "id": item["id"], "type": item["type"], "displayName": display_name,
            "definition": definition, "requires": item["requires"],
        })
        known.add(item["id"])
    migrations = []
    seen_migrations: set[str] = set()
    for raw in manifest["migrations"]:
        migration = fields(raw, {"id", "engine", "source", "requires", "recovery"}, "Migration")
        if migration["id"] in seen_migrations or migration["engine"] != "SPARK_SQL":
            raise ContractError("Invalid migration identity/engine")
        if set(migration["requires"]) - seen_migrations:
            raise ContractError("Migration prerequisite missing or out of order")
        sql = safe_file(repository, migration["source"]).read_text(encoding="utf-8")
        placeholders = set(re.findall(r"\{([A-Za-z_]+)\}", sql))
        if placeholders - {"control", "processing", "gold"}:
            raise ContractError("Unregistered migration placeholder")
        for role in placeholders:
            namespace = f"`{environment.workspace_name}`.`{environment.lakehouses[role].name}`"
            sql = sql.replace("{" + role + "}", namespace)
        if not migration["recovery"]:
            raise ContractError("Migration recovery procedure required")
        migrations.append({
            **migration, "source_sha256": hashlib.sha256(
                safe_file(repository, migration["source"]).read_bytes()).hexdigest(),
            "rendered_sql": sql,
        })
        seen_migrations.add(migration["id"])
    pending = manifest["pending_runtime_stages"]
    if not isinstance(pending, list) or not pending or any(not isinstance(s, str) for s in pending):
        raise ContractError("Pending runtime stage register must be explicit")
    plan = {
        "contract_version": "1.0", "status": "PLANNED_NOT_DEPLOYED",
        "scope": manifest["scope"], "project_id": environment.project_id,
        "tenant_id": environment.tenant_id, "workspace_id": environment.workspace_id,
        "project_release_id": project.release_id,
        "environment_fingerprint": fingerprint(environment_value),
        "wheel": {"name": wheel.name, "sha256": wheel_hash},
        "item_fingerprints": item_fingerprints, "items": definitions,
        "migrations": migrations,
        "unresolved_bindings": ["etl_environment_item_id"],
        "runtime_blockers": list(project.runtime_blockers),
        "pending_runtime_stages": pending,
        "delete_items": False, "replay_data": False,
    }
    plan["plan_id"] = fingerprint(plan)
    return plan
