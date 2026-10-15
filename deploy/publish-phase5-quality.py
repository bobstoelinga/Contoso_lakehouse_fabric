"""Upgrade only the owned dev phase-five wheel, preserving the base runtime."""

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


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--bundle", choices=("quality", "quality-window", "metadata-quality", "metadata-write", "exit"), default="quality")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    manifest_path = root / "fabric" / f"phase5-{args.bundle}-bundle.json"
    manifest = load_json(manifest_path)
    plan = build_phase5_plan(root, manifest_path)
    workspace = plan["workspace_id"]
    environment = plan["environment_binding"]["item_id"]
    if (
        workspace != "4be37544-10b9-450d-80f0-f9b3d793b83b"
        or environment != "7266a872-dd91-4aa6-a0b4-d4cc5b862643"
    ):
        raise ContractError("Quality runtime upgrade is restricted to the owned dev Environment")
    client = ItemClient(AzureCliTransport(plan["tenant_id"]), timeout_seconds=1200)
    client.start()
    client.assert_exclusive_setup(workspace)
    definition = client.definition(workspace, environment, "Environment")
    parts = decode_parts(definition)
    base = "Libraries/CustomLibraries/fabric_etl-0.1.0-py3-none-any.whl"
    predecessor, predecessor_sha = (
        ("0.6.0", "6c2c2a2679dd1435e4b3c6904c3d9f16ac532719315f757c9c1a5a41b5611d9c")
        if args.bundle == "exit"
        else ("0.4.0", "16f322fd0e4020d31dd6f3f6fa6d7557b4e5e5f22c2f5f6d8733a1bfb068c1eb")
        if args.bundle == "metadata-write"
        else ("0.3.0", "86b8b102e8ca59feaa12a364e287ca0525d7b1e5b98502c26e35669c4e59b0a7")
        if args.bundle == "metadata-quality"
        else ("0.2.0", "5b98ea1092fb580cb925fb2d0ba771b89b878cdcb4e101e2a9d85d9bff2dddc4")
        if args.bundle == "quality-window"
        else ("0.1.0", "081953584d6aadd7d32528ff2932ba0fdd749f8539e63c38a897d484863270a1")
    )
    old = f"Libraries/CustomLibraries/fabric_etl_phase5-{predecessor}-py3-none-any.whl"
    new = "Libraries/CustomLibraries/" + plan["environment_library"]["name"]
    settings = "Setting/Sparkcompute.yml"
    if set(parts) not in ({base, old, settings, ".platform"}, {base, new, settings, ".platform"}):
        raise ContractError("Environment inventory drift; refusing a replacement definition")
    if hashlib.sha256(parts[base]).hexdigest() != "b9da29e98d1d06440b3a04515c32d71a60913ce108d50a705033156b2bab0dbb":
        raise ContractError("Published base framework wheel differs from its previously verified pin")
    if hashlib.sha256(parts[settings]).hexdigest() != "878607076a0add4ac6d6ee77aa6a30e104b39ddb2103c6950c7db2eea7560593":
        raise ContractError("Published Spark settings differ from their previously verified pin")
    if new in parts and hashlib.sha256(parts[new]).hexdigest() != plan["environment_library"]["sha256"]:
        raise ContractError("Existing new add-on has different bytes; refusing publication")
    receipt = {
        "scope": "OWNED_DEV_PHASE5_QUALITY_RUNTIME_UPGRADE",
        "workspace_id": workspace, "environment_id": environment,
        "plan_id": plan["plan_id"], "library": plan["environment_library"],
        "status": "PREFLIGHT_VERIFIED",
        "preserved_sha256": {path: hashlib.sha256(parts[path]).hexdigest() for path in (base, settings)},
    }
    args.receipt.parent.mkdir(parents=True, exist_ok=True)
    if args.receipt.exists():
        previous = load_json(args.receipt)
        if previous.get("plan_id") != plan["plan_id"]:
            raise ContractError("Refusing to overwrite a receipt for a different upgrade plan")
    args.receipt.write_bytes(canonical_json(receipt))
    if old in parts:
        if hashlib.sha256(parts[old]).hexdigest() != predecessor_sha:
            raise ContractError("Published old add-on wheel differs; refusing the upgrade")
        wheel = (root / manifest["library"]["path"]).read_bytes()
        updated = {"parts": [
            {"path": path, "payloadType": "InlineBase64",
             "payload": base64.b64encode(data).decode("ascii")}
            for path, data in {base: parts[base], settings: parts[settings], new: wheel}.items()
        ]}
        receipt["status"] = "DEFINITION_UPDATE_REQUESTED"
        args.receipt.write_bytes(canonical_json(receipt))
        client.operation(client._request(
            "POST", f"https://api.fabric.microsoft.com/v1/workspaces/{workspace}"
            f"/environments/{environment}/updateDefinition",
            {"definition": updated},
        ), require_result=False)
        receipt["status"] = "PUBLICATION_REQUESTED"
        args.receipt.write_bytes(canonical_json(receipt))
        receipt["publish"] = client.publish_environment(workspace, environment)
    else:
        receipt["action"] = "VERIFIED_EXISTING_PUBLICATION"
    client.verify_published_library(
        workspace, environment, plan["environment_library"]["name"],
        plan["environment_library"]["sha256"],
    )
    observed = decode_parts(client.definition(workspace, environment, "Environment"))
    if set(observed) != {base, new, settings, ".platform"}:
        raise ContractError("Published upgraded Environment inventory differs")
    if any(observed[path] != parts[path] for path in (base, settings)):
        raise ContractError("Base framework or Spark settings changed during Quality upgrade")
    receipt["status"] = "PUBLISHED_READBACK_VERIFIED"
    args.receipt.write_bytes(canonical_json(receipt))
    print(json.dumps(receipt, indent=2))


if __name__ == "__main__":
    main()
