"""Repeatable phase-four platform proofs, with receipts and no business writes."""

from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fabric_etl import __version__
from fabric_etl.contracts import ContractError, Environment, fingerprint, load_json
from fabric_etl.control import Owner
from fabric_etl.deployment import API, DeploymentError, Transport
from fabric_etl.items import ItemClient, JobFailure


def save(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2), encoding="utf-8")
    temporary.replace(path)


def run_proofs(
    environment_path: Path, journal_path: Path, parameters_path: Path,
    evidence: Path, transport: Transport,
) -> dict:
    environment = Environment.from_dict(load_json(environment_path))
    environment.require_provisioned()
    journal = load_json(journal_path)
    if journal["workspace_id"] != environment.workspace_id:
        raise ContractError("Item journal is not bound to the proof workspace")
    items = journal["items"]
    for name in ("etl_environment", "verify_foundation", "setup_control", "owner_probe"):
        if name not in items or items[name].get("verified") is not True:
            raise ContractError("All phase-four item definitions must be verified before running proofs")
    environment_id = items["etl_environment"]["item_id"]
    control_id = environment.lakehouses["control"].item_id
    if control_id is None:
        raise ContractError("Control lakehouse binding missing")
    parameters = load_json(parameters_path)
    if not isinstance(parameters, dict) or parameters["environment_json"] != environment_path.read_text(encoding="utf-8"):
        raise ContractError("Proof parameter environment binding differs")
    results = {}

    def run(name: str, item_name: str, values: dict, *, expected_failure: bool = False) -> dict:
        item_id = items[item_name]["item_id"]
        path = evidence / f"{name}.json"
        client = ItemClient(transport, timeout_seconds=1800)
        client.start()
        if path.exists():
            receipt = load_json(path)
            if (receipt["parameters_fingerprint"] != fingerprint(values)
                    or receipt["item_id"] != item_id or receipt.get("plan_id") != journal["plan_id"]):
                raise ContractError("Existing job receipt differs; select a new proof execution folder")
            url = receipt["job_url"]
        else:
            url = client.submit_notebook(environment.workspace_id, item_id, values,
                                         environment_id=environment_id, lakehouse_id=control_id)
            receipt = {"workspace_id": environment.workspace_id, "item_id": item_id,
                       "plan_id": journal["plan_id"],
                       "job_url": url, "parameters_fingerprint": fingerprint(values), "status": "SUBMITTED"}
            save(path, receipt)
        print(json.dumps({"proof": name, "job_url": url, "status": receipt["status"]}), flush=True)
        try:
            job = client.wait_job(url)
        except JobFailure as exc:
            receipt.update(status=exc.job["status"], job=exc.job)
            save(path, receipt)
            if not expected_failure:
                raise
            return receipt
        if expected_failure:
            raise ContractError("Intentional stopped-writer job unexpectedly completed")
        receipt.update(status="Completed", job=job)
        save(path, receipt)
        print(json.dumps({"proof": name, "exit_status": job["exit_result"]["status"]}), flush=True)
        return receipt

    client = ItemClient(transport, timeout_seconds=600)
    client.start()
    client.assert_exclusive_setup(environment.workspace_id)
    for name in ("01-setup", "02-setup-retry"):
        result = run(name, "setup_control", parameters)["job"]["exit_result"]
        if result["status"] != "SUCCEEDED" or not result["batch_retry_identical"] or not result["owner_released"]:
            raise ContractError("Control setup/retry proof did not meet its exit contract")
        results[name] = result
    if results["01-setup"]["first_batch"] != results["02-setup-retry"]["first_batch"]:
        raise ContractError("Cross-session batch time/identity retry differs")
    if any(migration["action"] != "verified" for migration in results["02-setup-retry"]["migrations"]):
        raise ContractError("Second migration execution was not purely verified")

    base = {"environment_json": environment_path.read_text(encoding="utf-8"),
            "expected_framework_version": __version__, "mode": "", "start_utc": "",
            "owner_json": "", "terminal_proof_json": ""}
    race_file = evidence / "race-barrier.json"
    if race_file.exists():
        barrier = load_json(race_file)["start_utc"]
    else:
        barrier = (datetime.now(timezone.utc) + timedelta(minutes=6)).isoformat()
        save(race_file, {"start_utc": barrier})
    race_values = {**base, "mode": "race", "start_utc": barrier}
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(run, name, "owner_probe", race_values)
                   for name in ("03-race-a", "04-race-b")]
        race = [future.result()["job"]["exit_result"] for future in futures]
    if sorted(value["status"] for value in race) != ["BLOCKED", "SUCCEEDED"]:
        raise ContractError("Concurrent writer proof requires exactly one winner and one blocked loser")
    if race[0]["ready_generation"] != race[1]["ready_generation"]:
        raise ContractError("Race jobs did not read the same FREE generation before the write barrier")
    results["race"] = race

    stopped = run("05-intentional-writer-stop", "owner_probe", {**base, "mode": "stop"}, expected_failure=True)
    inspected = run("06-owner-inspection", "owner_probe", {**base, "mode": "inspect"})["job"]["exit_result"]
    if inspected["owner_state"] != "HELD":
        raise ContractError("Stopped writer owner was not retained")
    owner = Owner(**inspected["owner"])
    blocked = run("07-no-ttl-takeover", "owner_probe", {**base, "mode": "probe"})["job"]["exit_result"]
    if blocked["status"] != "BLOCKED" or blocked["observed_owner"] != owner.owner_id:
        raise ContractError("Stopped writer was incorrectly taken over")
    client.start()
    client.assert_exclusive_setup(environment.workspace_id)
    session = client._get(
        f"{API}/workspaces/{environment.workspace_id}/notebooks/{items['owner_probe']['item_id']}"
        f"/livySessions/{owner.job_instance_id}",
    )
    job_id = stopped["job_url"].rstrip("/").split("/")[-1]
    if session.get("jobInstanceId") != job_id or session.get("state") not in {"Failed", "Cancelled", "Succeeded", "Completed"}:
        raise DeploymentError("Old Spark execution is not a verified terminal session")
    proof = {
        "workspace_id": environment.workspace_id, "item_id": items["owner_probe"]["item_id"],
        "livy_id": owner.job_instance_id, "job_instance_id": job_id,
        "livy_state": session["state"], "job_status": stopped["job"]["status"],
        "confirmed_at": datetime.now(timezone.utc).isoformat(),
    }
    proof_path = evidence / "terminal-writer-proof.json"
    if proof_path.exists():
        persisted = load_json(proof_path)
        if any(persisted.get(key) != value for key, value in proof.items() if key != "confirmed_at"):
            raise ContractError("Persisted terminal writer evidence differs from the platform")
        proof = persisted
    else:
        save(proof_path, proof)
    recovered = run("08-explicit-recovery", "owner_probe", {
        **base, "mode": "recover", "owner_json": json.dumps(inspected["owner"]),
        "terminal_proof_json": json.dumps(proof),
    })["job"]["exit_result"]
    final = run("09-final-owner-inspection", "owner_probe", {**base, "mode": "inspect"})["job"]["exit_result"]
    if recovered["status"] != "SUCCEEDED" or final["owner_state"] != "FREE":
        raise ContractError("Explicit stopped-writer recovery did not leave a FREE owner")
    results.update(stopped_writer=proof, no_takeover=blocked, recovery=recovered, final_owner=final)
    report = {"status": "SUCCEEDED", "scope": "PHASE4_CONTROL_FOUNDATION_ONLY",
              "workspace_id": environment.workspace_id, "framework_version": __version__,
              "plan_id": journal["plan_id"], "results": results,
              "business_data_written": False, "full_etl_or_redeploy_certified": False}
    save(evidence / "acceptance.json", report)
    return report
