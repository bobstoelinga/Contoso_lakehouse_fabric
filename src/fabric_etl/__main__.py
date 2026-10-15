"""Explicit validation and foundation deployment commands."""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import HTTPRedirectHandler, Request, build_opener
from uuid import UUID

from fabric_etl import __version__
from fabric_etl.contracts import ContractError, Environment, canonical_json, load_json
from fabric_etl.deployment import DeploymentError, FoundationDeployer, Response, api_url
from fabric_etl.contoso import convert
from fabric_etl.plan import build_plan
from fabric_etl.project import ProjectRelease
from fabric_etl.items import ItemClient, JobFailure, deploy_items
from fabric_etl.project import safe_file
from fabric_etl.phase4 import run_proofs
from fabric_etl.bundle import build_phase5_plan, deploy_phase5_plan


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class AzureCliTransport:
    def __init__(self, tenant_id: str) -> None:
        self.tenant_id = str(UUID(tenant_id))

    def __call__(self, method: str, url: str, body: object | None, timeout: float) -> Response:
        api_url(url)
        executable = shutil.which("az")
        if executable is None:
            raise DeploymentError("Azure CLI is required; authenticate with a licensed Fabric identity")
        deadline = time.monotonic() + timeout
        result = subprocess.run([
            executable, "account", "get-access-token",
            "--tenant", self.tenant_id,
            "--resource", "https://api.fabric.microsoft.com", "--output", "json",
        ], capture_output=True, text=True, check=False, timeout=timeout)
        if result.returncode:
            raise DeploymentError("Azure CLI token acquisition failed; inspect az account/login")
        token_result = json.loads(result.stdout)
        if not isinstance(token_result, dict):
            raise DeploymentError("Azure CLI token result has an invalid shape")
        token = token_result.get("accessToken")
        if not isinstance(token, str) or not token:
            raise DeploymentError("Azure CLI did not return an access token")
        request = Request(
            url, data=None if body is None else canonical_json(body), method=method,
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        )
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise DeploymentError("Token acquisition exceeded the request deadline")
        try:
            with build_opener(NoRedirect()).open(request, timeout=remaining) as response:
                payload = response.read()
                return Response(
                    response.status, json.loads(payload) if payload else None,
                    {key.lower(): value for key, value in response.headers.items()},
                )
        except HTTPError as exc:
            payload = exc.read()
            return Response(
                exc.code, json.loads(payload) if payload else None,
                {key.lower(): value for key, value in exc.headers.items()},
            )
        except URLError as exc:
            raise DeploymentError(
                "Fabric transport failed; remote completion is uncertain. Reconcile before retry."
            ) from exc


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m fabric_etl")
    commands = parser.add_subparsers(dest="command", required=True)
    validate = commands.add_parser("validate-json", help="Reject duplicate fields and invalid JSON")
    validate.add_argument("path", type=Path)
    environment = commands.add_parser("validate-environment")
    environment.add_argument("path", type=Path)
    foundation = commands.add_parser("deploy-foundation", help="Reconcile three lakehouses without deleting data")
    foundation.add_argument("path", type=Path)
    foundation.add_argument("--timeout-seconds", type=float, default=600)
    project = commands.add_parser("validate-project", help="Validate a complete candidate project release")
    project.add_argument("path", type=Path)
    project.add_argument("--require-runtime", action="store_true")
    conversion = commands.add_parser("convert-contoso", help="Convert reviewed v2 metadata without activating it")
    conversion.add_argument("path", type=Path, help="Existing metadata seed directory")
    conversion.add_argument("--output", type=Path, required=True)
    plan = commands.add_parser("plan-release", help="Build a local definition plan; never deploy or load data")
    plan.add_argument("path", type=Path, help="Project release JSON")
    plan.add_argument("--repository", type=Path, required=True)
    plan.add_argument("--environment", type=Path, required=True)
    plan.add_argument("--wheel", type=Path, required=True)
    plan.add_argument("--output", type=Path, required=True)
    bundle_plan = commands.add_parser(
        "plan-phase5-bundle", help="Build a local, fingerprinted plan for a supported phase-five bundle",
    )
    bundle_plan.add_argument("path", type=Path, help="Phase-five bundle manifest")
    bundle_plan.add_argument("--repository", type=Path, required=True)
    bundle_plan.add_argument("--output", type=Path, required=True)
    bundle_deploy = commands.add_parser(
        "deploy-phase5-bundle", help="Deploy an owned phase-five notebook after verifying its published wheel",
    )
    bundle_deploy.add_argument("path", type=Path, help="Fingerprint-validated phase-five plan")
    bundle_deploy.add_argument("--journal", type=Path, required=True)
    bundle_deploy.add_argument("--timeout-seconds", type=float, default=1200)
    items = commands.add_parser("deploy-items", help="Deploy and verify pinned phase-four items; no ETL replay")
    items.add_argument("path", type=Path, help="Validated plan")
    items.add_argument("--journal", type=Path, required=True)
    items.add_argument("--timeout-seconds", type=float, default=2400)
    run = commands.add_parser("run-notebook", help="Submit once, persist receipt, and monitor a bound notebook")
    run.add_argument("path", type=Path, help="Environment binding")
    run.add_argument("--item-id", required=True)
    run.add_argument("--environment-id", required=True)
    run.add_argument("--parameters", type=Path)
    run.add_argument("--receipt", type=Path, required=True)
    run.add_argument("--timeout-seconds", type=float, default=1800)
    run.add_argument("--exclusive-setup", action="store_true")
    setup = commands.add_parser("prepare-control", help="Prepare validated control-proof parameters without execution")
    setup.add_argument("path", type=Path, help="Environment binding")
    setup.add_argument("--project", type=Path, required=True)
    setup.add_argument("--repository", type=Path, required=True)
    setup.add_argument("--output", type=Path, required=True)
    proofs = commands.add_parser("prove-control", help="Execute phase-four setup, concurrency and stopped-writer proofs")
    proofs.add_argument("path", type=Path, help="Environment binding")
    proofs.add_argument("--journal", type=Path, required=True)
    proofs.add_argument("--parameters", type=Path, required=True)
    proofs.add_argument("--evidence", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == "prove-control":
            env = Environment.from_dict(load_json(args.path))
            report = run_proofs(args.path, args.journal, args.parameters, args.evidence,
                                AzureCliTransport(env.tenant_id))
            print(json.dumps({"status": report["status"], "scope": report["scope"],
                              "evidence": str(args.evidence)}, indent=2))
            return 0
        if args.command == "prepare-control":
            env = Environment.from_dict(load_json(args.path))
            env.require_provisioned()
            project = ProjectRelease.load(args.project)
            if env.project_id != project.document["project_id"]:
                raise ContractError("Project/control environment mismatch")
            migrations = []
            import hashlib
            for record in load_json(args.repository / "fabric" / "deployment.json")["migrations"]:
                source = safe_file(args.repository, record["source"]).read_bytes().decode("utf-8")
                migrations.append({"id": record["id"], "source_sql": source,
                                   "source_sha256": hashlib.sha256(source.encode()).hexdigest()})
            parameters = {
                "environment_json": args.path.read_text(encoding="utf-8"),
                "expected_framework_version": __version__,
                "migrations_json": json.dumps(migrations),
                "project_json": json.dumps(project.document),
                "files_json": json.dumps({
                    relative: safe_file(args.project.parent, relative).read_bytes().decode("utf-8")
                    for relative in project.document["files"]
                }),
            }
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(parameters, indent=2), encoding="utf-8")
            print(json.dumps({"status": "PARAMETERS_PREPARED_NOT_EXECUTED", "output": str(args.output)}))
            return 0
        if args.command == "run-notebook":
            env = Environment.from_dict(load_json(args.path))
            client = ItemClient(AzureCliTransport(env.tenant_id), timeout_seconds=args.timeout_seconds)
            client.start()
            if args.receipt.exists():
                receipt = load_json(args.receipt)
                if receipt["item_id"] != args.item_id or receipt["workspace_id"] != env.workspace_id:
                    raise ContractError("Existing notebook receipt binding differs")
                url = receipt["job_url"]
            else:
                if args.exclusive_setup:
                    client.assert_exclusive_setup(env.workspace_id)
                parameters = load_json(args.parameters) if args.parameters else {
                    "environment_json": args.path.read_text(encoding="utf-8"),
                    "expected_framework_version": __version__,
                }
                if not isinstance(parameters, dict) or any(not isinstance(v, str) for v in parameters.values()):
                    raise ContractError("Notebook parameters must be string-valued")
                url = client.submit_notebook(
                    env.workspace_id, str(UUID(args.item_id)), parameters,
                    environment_id=args.environment_id,
                    lakehouse_id=str(UUID(env.lakehouses["control"].item_id or "")),
                )
                receipt = {
                    "workspace_id": env.workspace_id, "item_id": args.item_id,
                    "job_url": url, "status": "SUBMITTED",
                }
                args.receipt.parent.mkdir(parents=True, exist_ok=True)
                args.receipt.write_text(json.dumps(receipt, indent=2), encoding="utf-8")
            print(json.dumps(receipt), flush=True)
            try:
                receipt["job"] = client.wait_job(url)
            except JobFailure as exc:
                receipt["status"] = exc.job["status"].upper()
                receipt["job"] = exc.job
                args.receipt.write_text(json.dumps(receipt, indent=2), encoding="utf-8")
                raise
            receipt["status"] = "COMPLETED"
            args.receipt.write_text(json.dumps(receipt, indent=2), encoding="utf-8")
            print(json.dumps(receipt, indent=2))
            return 0
        if args.command == "deploy-items":
            value = load_json(args.path)
            outcome = deploy_items(ItemClient(
                AzureCliTransport(value["tenant_id"]), timeout_seconds=args.timeout_seconds,
            ), value, args.journal)
            print(json.dumps(outcome, indent=2))
            return 0
        if args.command == "plan-phase5-bundle":
            result = build_phase5_plan(args.repository, args.path)
            text = json.dumps(result, indent=2) + "\n"
            if args.output.exists() and args.output.read_text(encoding="utf-8") != text:
                raise ContractError("Refusing to overwrite a different phase-five plan")
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(text, encoding="utf-8", newline="\n")
            print(json.dumps({
                "status": result["status"], "scope": result["scope"],
                "bundle_id": result["bundle_id"], "plan_id": result["plan_id"],
                "workspace_id": result["workspace_id"],
                "environment_id": result["environment_binding"]["item_id"],
                "items": [item["displayName"] for item in result["items"]],
                "library": result["environment_library"],
                "runtime_blockers": result["runtime_blockers"],
                "output": str(args.output),
            }, indent=2))
            return 0
        if args.command == "deploy-phase5-bundle":
            value = load_json(args.path)
            if not isinstance(value, dict):
                raise ContractError("Phase-five plan must be a JSON object")
            outcome = deploy_phase5_plan(ItemClient(
                AzureCliTransport(value["tenant_id"]), timeout_seconds=args.timeout_seconds,
            ), value, args.journal)
            print(json.dumps(outcome, indent=2))
            return 0
        if args.command == "convert-contoso":
            release = convert(args.path, args.output)
            print(json.dumps({"status": "CANDIDATE_GENERATED", "release_id": release["release_id"]}))
            return 0
        if args.command == "validate-project":
            release = ProjectRelease.load(args.path, require_runtime=args.require_runtime)
            print(json.dumps({
                "status": "VALID_CANDIDATE" if release.runtime_blockers else "VALID",
                "release_id": release.release_id,
                "collections": {key: len(val) for key, val in release.document.items() if isinstance(val, list)},
                "runtime_blockers": list(release.runtime_blockers),
            }, indent=2))
            return 0
        if args.command == "plan-release":
            result = build_plan(args.repository, args.path, args.environment, args.wheel)
            text = json.dumps(result, indent=2) + "\n"
            if args.output.exists() and args.output.read_text(encoding="utf-8") != text:
                raise ContractError("Refusing to overwrite a different deployment plan")
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(text, encoding="utf-8", newline="\n")
            print(json.dumps({
                "status": result["status"], "scope": result["scope"],
                "plan_id": result["plan_id"], "output": str(args.output),
                "runtime_blocker_count": len(result["runtime_blockers"]),
                "unresolved_bindings": result["unresolved_bindings"],
            }, indent=2))
            return 0
        value = load_json(args.path)
        if args.command == "validate-json":
            print(json.dumps({"status": "VALID", "scope": "JSON syntax and duplicate fields only"}))
        else:
            binding = Environment.from_dict(value)
            if args.command == "validate-environment":
                print(json.dumps({
                    "status": "VALID", "project_id": binding.project_id,
                    "environment": binding.environment,
                    "item_ids_bound": all(item.item_id for item in binding.lakehouses.values()),
                }))
            else:
                outcomes = FoundationDeployer(
                    AzureCliTransport(binding.tenant_id), timeout_seconds=args.timeout_seconds,
                ).reconcile(binding)
                print(json.dumps({
                    "status": "SUCCEEDED", "scope": "foundation only",
                    "workspace_id": binding.workspace_id, "lakehouses": outcomes,
                }, indent=2))
    except (
        ContractError, DeploymentError, OSError, json.JSONDecodeError,
        subprocess.SubprocessError, ValueError,
    ) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
