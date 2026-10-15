"""Publish a pinned Sales extension while preserving the base and ingestion runtimes."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
from pathlib import Path

from fabric_etl.__main__ import AzureCliTransport
from fabric_etl.bundle import build_phase5_plan
from fabric_etl.contracts import ContractError, canonical_json, load_json
from fabric_etl.items import ItemClient, decode_parts


PRESERVED_SHA256 = {
    "Libraries/CustomLibraries/fabric_etl-0.1.0-py3-none-any.whl":
        "b9da29e98d1d06440b3a04515c32d71a60913ce108d50a705033156b2bab0dbb",
    "Libraries/CustomLibraries/fabric_etl_phase5-0.6.1-py3-none-any.whl":
        "7211506d031508eb7942f2b6a95a10bc7679fea5f90513604ce809bf792097a9",
    "Setting/Sparkcompute.yml": "878607076a0add4ac6d6ee77aa6a30e104b39ddb2103c6950c7db2eea7560593",
}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--bundle", type=Path, default=Path("fabric/phase6-sales-quality-bundle.json"))
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    manifest_path = root / args.bundle
    manifest = load_json(manifest_path)
    plan = build_phase5_plan(root, manifest_path)
    workspace = plan["workspace_id"]
    environment = plan["environment_binding"]["item_id"]
    if workspace != "4be37544-10b9-450d-80f0-f9b3d793b83b" or environment != "7266a872-dd91-4aa6-a0b4-d4cc5b862643":
        raise ContractError("Sales extension publication is restricted to the owned dev Environment")
    preserved = PRESERVED_SHA256
    new = "Libraries/CustomLibraries/" + plan["environment_library"]["name"]
    predecessor = None
    predecessors = {
        "contoso_phase6_sales_vault": "phase6-sales-quality-bundle.json",
        "contoso_phase6_sales_gold": "phase6-sales-vault-bundle.json",
    }
    if manifest["bundle_id"] == "contoso_phase6_sales_gold" and manifest["library"]["version"] == "0.3.1":
        predecessors["contoso_phase6_sales_gold"] = "phase6-sales-gold-bundle.json"
    if manifest["bundle_id"] in predecessors:
        previous = load_json(root / "fabric" / predecessors[manifest["bundle_id"]])["library"]
        predecessor = "Libraries/CustomLibraries/" + Path(previous["path"]).name
    client = ItemClient(AzureCliTransport(plan["tenant_id"]), timeout_seconds=1800)
    client.start()
    client.assert_exclusive_setup(workspace)
    parts = decode_parts(client.definition(workspace, environment, "Environment"))
    allowed_initial = set(preserved) | {".platform"}
    if predecessor:
        allowed_initial.add(predecessor)
    if set(parts) not in (allowed_initial, set(preserved) | {".platform", new}):
        raise ContractError("Environment inventory drift; refusing the Sales publication")
    if predecessor in parts and hashlib.sha256(parts[predecessor]).hexdigest() != previous["sha256"]:
        raise ContractError("Sales predecessor differs from its immutable published pin")
    if any(hashlib.sha256(parts[path]).hexdigest() != digest for path, digest in preserved.items()):
        raise ContractError("Existing framework, ingestion runtime or Spark settings differ from their pins")
    wheel = (root / manifest["library"]["path"]).read_bytes()
    if new in parts and parts[new] != wheel:
        raise ContractError("Immutable Sales library already exists with different bytes")
    receipt = {
        "scope": "OWNED_DEV_PHASE6_SALES_EXTENSION", "plan_id": plan["plan_id"],
        "workspace_id": workspace, "environment_id": environment,
        "library": plan["environment_library"], "preserved_sha256": preserved,
        "superseded_library": previous if predecessor else None,
        "status": "PREFLIGHT_VERIFIED",
    }
    if args.receipt.exists() and load_json(args.receipt)["plan_id"] != plan["plan_id"]:
        raise ContractError("Sales publication receipt belongs to a different plan")
    args.receipt.parent.mkdir(parents=True, exist_ok=True)
    args.receipt.write_bytes(canonical_json(receipt))
    if new not in parts:
        receipt["status"] = "DEFINITION_UPDATE_REQUESTED"
        args.receipt.write_bytes(canonical_json(receipt))
        updated = {"parts": [
            {"path": path, "payloadType": "InlineBase64", "payload": base64.b64encode(data).decode("ascii")}
            for path, data in {**{path: parts[path] for path in preserved}, new: wheel}.items()
        ]}
        client.operation(client._request(
            "POST", f"https://api.fabric.microsoft.com/v1/workspaces/{workspace}/environments/{environment}/updateDefinition",
            {"definition": updated},
        ), require_result=False)
        receipt["status"] = "PUBLICATION_REQUESTED"
        args.receipt.write_bytes(canonical_json(receipt))
        receipt["publish"] = client.publish_environment(workspace, environment)
    client.verify_published_library(workspace, environment, plan["environment_library"]["name"],
                                    plan["environment_library"]["sha256"])
    observed = decode_parts(client.definition(workspace, environment, "Environment"))
    if set(observed) != set(preserved) | {".platform", new} or any(
        observed[path] != parts[path] for path in preserved
    ):
        raise ContractError("Published Sales inventory changed an existing runtime or settings")
    receipt["status"] = "PUBLISHED_READBACK_VERIFIED"
    args.receipt.write_bytes(canonical_json(receipt))
    print(json.dumps(receipt, indent=2))


if __name__ == "__main__":
    main()
