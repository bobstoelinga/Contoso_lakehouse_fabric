"""Small, fingerprinted phase-five bundle for a non-destructive runtime probe."""

from __future__ import annotations

import base64
import hashlib
import json
import zipfile
from email.parser import BytesParser
from pathlib import Path
from typing import Any
from uuid import UUID

from fabric_etl import __version__
from fabric_etl.contracts import (
    ContractError, Environment, fields, fingerprint, load_json, required_text,
)
from fabric_etl.items import ItemClient, decode_parts, deploy_items
from fabric_etl.project import safe_file


SUPPORTED_BUNDLES = {
    "contoso_phase6_sales_gold": {
        "scope": "PHASE6_SALES_GOLD_PROOF",
        "notebook_id": "phase6_sales_gold",
        "runtime_blockers": ["EXTENDED_SCENARIOS_AND_REFERENCE_COMPARISON"],
    },
    "contoso_phase6_sales_vault": {
        "scope": "PHASE6_SALES_VAULT_PROOF",
        "notebook_id": "phase6_sales_vault",
        "runtime_blockers": ["PIT_GOLD_EXTENDED_SCENARIOS_AND_REFERENCE_COMPARISON"],
    },
    "contoso_phase6_sales_quality": {
        "scope": "PHASE6_SALES_QUALITY_PROOF",
        "notebook_id": "phase6_sales_quality",
        "runtime_blockers": ["SALES_VAULT_BV_PIT_GOLD_AND_REFERENCE_COMPARISON"],
    },
    "contoso_phase5_exit_probe": {
        "scope": "PHASE5_EXIT_PROBE",
        "notebook_id": "phase5_exit_probe",
        "runtime_blockers": ["COMPLETE_SALES_CHAIN_AND_BUSINESS_ACCEPTANCE"],
    },
    "contoso_phase5_metadata_write_probe": {
        "scope": "PHASE5_METADATA_WRITE_PROBE",
        "notebook_id": "phase5_metadata_write_probe",
        "runtime_blockers": ["COMMITTED_REFERENCES_FULL_PROJECT_RUNTIME_AND_BUSINESS_ACCEPTANCE"],
    },
    "contoso_phase5_metadata_quality_probe": {
        "scope": "PHASE5_METADATA_QUALITY_PROBE",
        "notebook_id": "phase5_metadata_quality_probe",
        "runtime_blockers": ["PROJECT_RELEASE_BUSINESS_SOURCES_PUBLICATION_AND_SECURITY_ACCEPTANCE"],
    },
    "contoso_phase5_sales_source_probe": {
        "scope": "PHASE5_SALES_SOURCE_PROBE",
        "notebook_id": "phase5_sales_source_probe",
        "runtime_blockers": ["PROJECT_RELEASE_BUSINESS_SOURCES_PUBLICATION_AND_SECURITY_ACCEPTANCE"],
    },
    "contoso_phase5_quality_window_probe": {
        "scope": "PHASE5_QUALITY_WINDOW_PROBE",
        "notebook_id": "phase5_quality_window_probe",
        "runtime_blockers": ["PROJECT_RELEASE_BUSINESS_SOURCES_PUBLICATION_AND_SECURITY_ACCEPTANCE"],
    },
    "contoso_phase5_runtime_probe": {
        "scope": "PHASE5_RUNTIME_PROBE",
        "notebook_id": "phase5_runtime_probe",
        "runtime_blockers": ["QUALITY_REJECT_VAULT_GOLD_AND_SECURITY_ACCEPTANCE"],
    },
    "contoso_phase5_bronze_probe": {
        "scope": "PHASE5_BRONZE_PROBE",
        "notebook_id": "phase5_bronze_probe",
        "runtime_blockers": ["QUALITY_REJECT_VAULT_GOLD_AND_SECURITY_ACCEPTANCE"],
    },
    "contoso_phase5_quality_probe": {
        "scope": "PHASE5_QUALITY_PROBE",
        "notebook_id": "phase5_quality_probe",
        "runtime_blockers": ["VAULT_GOLD_PROJECT_RELEASE_AND_SECURITY_ACCEPTANCE"],
    },
    "contoso_phase5_chain_probe": {
        "scope": "PHASE5_CHAIN_PROBE",
        "notebook_id": "phase5_chain_probe",
        "runtime_blockers": ["PROJECT_RELEASE_BUSINESS_SOURCES_PUBLICATION_AND_SECURITY_ACCEPTANCE"],
    },
    "contoso_phase5_recovery_probe": {
        "scope": "PHASE5_RECOVERY_PROBE",
        "notebook_id": "phase5_recovery_probe",
        "runtime_blockers": ["PROJECT_RELEASE_BUSINESS_SOURCES_PUBLICATION_AND_SECURITY_ACCEPTANCE"],
    },
}


def build_phase5_plan(
    repository: Path, bundle_path: Path, *, expected_framework_version: str = __version__,
) -> dict[str, Any]:
    manifest = fields(load_json(bundle_path), {
        "contract_version", "bundle_id", "scope", "environment_binding",
        "library", "notebooks",
    }, "Phase-five bundle")
    if manifest["contract_version"] != "1.0":
        raise ContractError("Unsupported phase-five bundle contract")
    if expected_framework_version != __version__:
        raise ContractError("Expected framework version differs from this runner")
    bundle_id = required_text(manifest["bundle_id"], "bundle_id")
    bundle_spec = SUPPORTED_BUNDLES.get(bundle_id)
    if bundle_spec is None or manifest["scope"] != bundle_spec["scope"]:
        raise ContractError("Unsupported phase-five bundle identity")

    binding = fields(manifest["environment_binding"], {"path", "item_id"}, "Environment binding")
    environment_document = load_json(safe_file(repository, binding["path"]))
    environment = Environment.from_dict(environment_document)
    environment.require_provisioned()
    environment_id = str(UUID(required_text(binding["item_id"], "environment item_id")))

    library = fields(manifest["library"], {"path", "name", "version", "sha256"}, "Bundle library")
    quality_bundles = {
        "contoso_phase5_quality_probe", "contoso_phase5_chain_probe",
        "contoso_phase5_recovery_probe", "contoso_phase5_quality_window_probe",
        "contoso_phase5_sales_source_probe",
        "contoso_phase5_metadata_quality_probe",
        "contoso_phase5_metadata_write_probe",
        "contoso_phase5_exit_probe",
        "contoso_phase6_sales_quality",
        "contoso_phase6_sales_vault",
        "contoso_phase6_sales_gold",
    }
    supported_version = (
        "0.3.0" if bundle_id == "contoso_phase6_sales_gold"
        else "0.2.0" if bundle_id == "contoso_phase6_sales_vault"
        else "0.1.0" if bundle_id == "contoso_phase6_sales_quality"
        else "0.6.1" if bundle_id == "contoso_phase5_exit_probe"
        else "0.5.0" if bundle_id == "contoso_phase5_metadata_write_probe"
        else "0.4.0" if bundle_id == "contoso_phase5_metadata_quality_probe"
        else "0.3.0" if bundle_id in {"contoso_phase5_quality_window_probe", "contoso_phase5_sales_source_probe"}
        else "0.2.0" if bundle_id in quality_bundles else "0.1.0"
    )
    package_name = "fabric-etl-phase6" if bundle_id.startswith("contoso_phase6_sales_") else "fabric-etl-phase5"
    supported_versions = {"0.3.0", "0.3.1"} if bundle_id == "contoso_phase6_sales_gold" else {supported_version}
    if library["name"] != package_name or library["version"] not in supported_versions:
        raise ContractError("Unsupported phase-five library identity")
    wheel = safe_file(repository, library["path"])
    wheel_bytes = wheel.read_bytes()
    wheel_hash = hashlib.sha256(wheel_bytes).hexdigest()
    if wheel_hash != library["sha256"]:
        raise ContractError("Phase-five wheel SHA-256 differs from the bundle pin")
    try:
        with zipfile.ZipFile(wheel) as archive:
            metadata_paths = [name for name in archive.namelist() if name.endswith(".dist-info/METADATA")]
            if len(metadata_paths) != 1:
                raise ContractError("Phase-five wheel must contain one METADATA file")
            metadata = BytesParser().parsebytes(archive.read(metadata_paths[0]))
            if metadata["Name"] != library["name"] or metadata["Version"] != library["version"]:
                raise ContractError("Phase-five wheel metadata differs from the bundle pin")
            required = {"fabric_etl_phase5/__init__.py", "fabric_etl_phase5/ingestion.py"}
            if bundle_id.startswith("contoso_phase6_sales_"):
                required = {"fabric_etl_phase6/__init__.py", "fabric_etl_phase6/quality.py"}
            if bundle_id == "contoso_phase6_sales_vault":
                required.update({"fabric_etl_phase6/vault.py", "fabric_etl_phase6/proof.py"})
            elif bundle_id == "contoso_phase6_sales_gold":
                required.update({
                    "fabric_etl_phase6/vault.py", "fabric_etl_phase6/proof.py",
                    "fabric_etl_phase6/pit.py", "fabric_etl_phase6/gold.py", "fabric_etl_phase6/publication.py",
                })
            elif bundle_id in quality_bundles and not bundle_id.startswith("contoso_phase6_sales_"):
                required.add("fabric_etl_phase5/quality.py")
            if bundle_id == "contoso_phase5_exit_probe":
                required.add("fabric_etl_phase5/delivery.py")
            if bundle_id in {"contoso_phase5_metadata_quality_probe", "contoso_phase5_metadata_write_probe"}:
                required.add("fabric_etl_phase5/mapping.py")
            if not required.issubset(archive.namelist()):
                raise ContractError("Phase-five wheel is missing runtime modules")
    except zipfile.BadZipFile as exc:
        raise ContractError("Phase-five wheel is not a valid ZIP distribution") from exc

    notebooks = manifest["notebooks"]
    if not isinstance(notebooks, list) or len(notebooks) != 1:
        raise ContractError("Phase-five bundle requires exactly one notebook")
    notebook = fields(notebooks[0], {"id", "display_name", "source"}, "Bundle notebook")
    if notebook["id"] != bundle_spec["notebook_id"]:
        raise ContractError("Bundle notebook identity differs from its closed scope")
    source_path = safe_file(repository, notebook["source"])
    source = source_path.read_text(encoding="utf-8")
    if bundle_id == "contoso_phase6_sales_gold":
        version_parameter = 'expected_phase6_version = "0.3.1"'
        if source.count(version_parameter) != 1:
            raise ContractError("Sales Gold proof requires one exact runtime version parameter")
        source = source.replace(version_parameter, "expected_phase6_version = " + json.dumps(library["version"]), 1)
    if bundle_id in {"contoso_phase5_metadata_quality_probe", "contoso_phase5_metadata_write_probe",
                     "contoso_phase6_sales_quality", "contoso_phase6_sales_vault", "contoso_phase6_sales_gold"}:
        from fabric_etl.project import ProjectRelease
        project = ProjectRelease.load(repository / "projects" / "contoso" / "release.json")
        placeholder = 'project_json = "{}"'
        if source.count(placeholder) != 1:
            raise ContractError("Metadata probe requires one pinned project parameter")
        source = source.replace(placeholder, "project_json = " + json.dumps(
            json.dumps(project.document, sort_keys=True, separators=(",", ":")),
        ), 1)
    if bundle_id == "contoso_phase6_sales_vault":
        receipt_path = repository / "deploy" / "evidence" / "phase6-sales-quality-v1-receipt.json"
        receipt = load_json(receipt_path)
        if (receipt.get("status") != "COMPLETED" or receipt.get("job", {}).get("status") != "Completed"
                or receipt["job"].get("exit_result", {}).get("scope") != "PHASE6_SALES_COMMITTED_QUALITY"
                or receipt["job"]["exit_result"].get("status") != "SUCCEEDED"):
            raise ContractError("Sales Vault proof requires the completed committed-Quality input receipt")
        placeholder = 'quality_receipt_json = "{}"'
        if source.count(placeholder) != 1:
            raise ContractError("Sales Vault proof requires one pinned Quality receipt parameter")
        source = source.replace(placeholder, "quality_receipt_json = " + json.dumps(
            json.dumps(receipt["job"]["exit_result"], sort_keys=True, separators=(",", ":")),
        ), 1)
    if bundle_id == "contoso_phase6_sales_gold":
        receipt = load_json(repository / "deploy" / "evidence" / "phase6-sales-vault-v1-receipt.json")
        exit_result = receipt.get("job", {}).get("exit_result", {})
        if (receipt.get("status") != "COMPLETED" or receipt.get("job", {}).get("status") != "Completed"
                or exit_result.get("scope") != "PHASE6_SALES_COMMITTED_VAULT"
                or exit_result.get("status") != "SUCCEEDED" or exit_result.get("release_id") != project.release_id):
            raise ContractError("Sales Gold proof requires the completed committed-Vault input receipt")
        expressions = {expression["id"]: expression for expression in project.document["expressions"]}
        sql = {}
        for entity in project.document["gold_entities"]:
            if entity["source_system_id"] == "SALES":
                expression = expressions[entity["expression_ref"]]
                content = safe_file(repository / "projects" / "contoso", expression["path"]).read_bytes()
                if hashlib.sha256(content).hexdigest() != project.document["files"][expression["path"]]:
                    raise ContractError("Sales Gold proof SQL differs from its project checksum")
                sql[expression["id"]] = content.decode("utf-8")
        for parameter, value in (("vault_receipt_json", exit_result), ("gold_sources_json", sql)):
            placeholder = parameter + ' = "{}"'
            if source.count(placeholder) != 1:
                raise ContractError("Sales Gold proof requires one pinned parameter: " + parameter)
            source = source.replace(placeholder, parameter + " = " + json.dumps(
                json.dumps(value, sort_keys=True, separators=(",", ":")),
            ), 1)
    if not source.startswith("# Fabric notebook source") or source.count('"dependencies": {}') != 1:
        raise ContractError("Phase-five notebook must use the declared Fabric Git source format")
    if "${etl_environment_item_id}" in source:
        raise ContractError("Notebook source contains an unresolved Environment placeholder")
    if bundle_id == "contoso_phase5_bronze_probe" or bundle_id in quality_bundles:
        parameter = 'environment_json = "{}"'
        if source.count(parameter) != 1:
            raise ContractError("Write probe must declare exactly one Environment parameter")
        environment_json = json.dumps(
            json.dumps(environment_document, sort_keys=True, separators=(",", ":")),
        )
        source = source.replace(parameter, f"environment_json = {environment_json}", 1)
    dependency = {
        "environment": {
            "environmentId": "${etl_environment_item_id}",
            "workspaceId": environment.workspace_id,
        },
    }
    source = source.replace(
        '"dependencies": {}',
        '"dependencies": ' + json.dumps(dependency),
        1,
    )
    display_name = required_text(notebook["display_name"], "notebook display_name").format(
        project_id=environment.project_id, environment=environment.environment,
    )
    item_definition = {
        "format": "fabricGitSource",
        "parts": [{
            "path": "notebook-content.py",
            "payloadType": "InlineBase64",
            "payload": base64.b64encode(source.encode("utf-8")).decode("ascii"),
        }],
    }
    plan: dict[str, Any] = {
        "status": "PLANNED_NOT_DEPLOYED",
        "scope": manifest["scope"],
        "bundle_id": bundle_id,
        "tenant_id": environment.tenant_id,
        "workspace_id": environment.workspace_id,
        "environment_binding": {
            "item_id": environment_id,
            "workspace_id": environment.workspace_id,
        },
        "environment_library": {
            "name": wheel.name,
            "sha256": wheel_hash,
        },
        "items": [{
            "id": notebook["id"],
            "type": "Notebook",
            "displayName": display_name,
            "definition": item_definition,
            "requires": [],
        }],
        "runtime_blockers": bundle_spec["runtime_blockers"],
    }
    plan["plan_id"] = fingerprint(plan)
    return plan


def deploy_phase5_plan(client: ItemClient, plan: dict[str, Any], journal: Path) -> dict[str, Any]:
    expected = fingerprint({key: value for key, value in plan.items() if key != "plan_id"})
    bundle_spec = SUPPORTED_BUNDLES.get(plan.get("bundle_id"))
    if (
        plan.get("plan_id") != expected
        or plan.get("status") != "PLANNED_NOT_DEPLOYED"
        or bundle_spec is None
        or plan.get("scope") != bundle_spec["scope"]
        or not isinstance(plan.get("environment_binding"), dict)
        or not isinstance(plan.get("environment_library"), dict)
        or not isinstance(plan.get("items"), list)
        or len(plan["items"]) != 1
        or plan["items"][0].get("id") != bundle_spec["notebook_id"]
        or plan["items"][0].get("type") != "Notebook"
    ):
        raise ContractError("Invalid phase-five plan fingerprint, status, or scope")
    workspace_id = str(UUID(plan["workspace_id"]))
    environment_binding = plan["environment_binding"]
    if environment_binding.get("workspace_id") != workspace_id:
        raise ContractError("Phase-five notebook Environment workspace mismatch")
    client.start()
    client.verify_published_library(
        workspace_id,
        str(UUID(environment_binding["item_id"])),
        plan["environment_library"]["name"],
        plan["environment_library"]["sha256"],
    )
    return deploy_items(client, plan, journal)
