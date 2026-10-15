import json
from pathlib import Path

import pytest

from fabric_etl.contracts import ContractError, load_json
from fabric_etl.deployment import API
from fabric_etl.items import JobFailure
from fabric_etl.phase4 import run_proofs, save


ROOT = Path(__file__).resolve().parents[1]
WORKSPACE = "4be37544-10b9-450d-80f0-f9b3d793b83b"
ITEM = "91ec449c-1462-4a48-8fbe-7b146a8a5381"
LIVY = "09ba2e98-b078-496a-997c-ce3b138ba829"
OWNER = {"project_id": "contoso", "owner_id": "proof-owner", "generation": 3, "job_instance_id": LIVY}


def inputs(tmp_path):
    environment = ROOT / "environments" / "contoso.dev.json"
    journal = tmp_path / "journal.json"
    save(journal, {"workspace_id": WORKSPACE, "plan_id": "proof-plan", "items": {
        name: {"verified": True, "item_id": ITEM}
        for name in ("etl_environment", "verify_foundation", "setup_control", "owner_probe")
    }})
    parameters = tmp_path / "parameters.json"
    save(parameters, {"environment_json": environment.read_text(encoding="utf-8")})
    return environment, journal, parameters, tmp_path / "evidence"


def test_proofs_require_all_verified_items_before_network(tmp_path):
    paths = inputs(tmp_path)
    journal = load_json(paths[1])
    journal["items"]["owner_probe"]["verified"] = False
    save(paths[1], journal)
    with pytest.raises(ContractError, match="verified"):
        run_proofs(*paths, lambda *args: pytest.fail("No network before validation"))


def test_proof_sequence_receipts_and_terminal_evidence_are_reused(tmp_path, monkeypatch):
    jobs = {}
    counts = {"setup": 0, "race": 0, "inspect": 0}
    stopped_job = None

    class Client:
        def __init__(self, *args, **kwargs):
            pass
        def start(self):
            pass
        def assert_exclusive_setup(self, workspace):
            assert workspace == WORKSPACE
        def submit_notebook(self, workspace, item, values, **kwargs):
            nonlocal stopped_job
            number = len(jobs) + 1
            mode = values.get("mode", "setup")
            url = f"{API}/workspaces/{workspace}/items/{item}/jobs/instances/{number:08d}-0000-0000-0000-000000000000"
            if mode == "setup":
                counts["setup"] += 1
                result = {"status": "SUCCEEDED", "scope": "PHASE4_CONTROL_PROOF_NO_BUSINESS_INGESTION",
                          "batch_retry_identical": True, "owner_released": True, "first_batch": {"id": "stable"},
                          "migrations": [{"action": "applied" if counts["setup"] == 1 else "verified"}]}
            elif mode == "race":
                counts["race"] += 1
                result = {"status": "SUCCEEDED" if counts["race"] == 1 else "BLOCKED",
                          "scope": "OWNER_RACE", "ready_generation": 2}
            elif mode == "stop":
                stopped_job = url.rsplit("/", 1)[1]
                jobs[url] = {"status": "Failed", "failureReason": {"message": "Intentional stop"}}
                return url
            elif mode == "inspect":
                counts["inspect"] += 1
                result = {"status": "SUCCEEDED", "scope": "READ_ONLY_OWNER_INSPECTION",
                          "owner": OWNER, "owner_state": "HELD" if counts["inspect"] == 1 else "FREE"}
            elif mode == "probe":
                result = {"status": "BLOCKED", "scope": "OWNER_NO_TTL_TAKEOVER",
                          "observed_owner": OWNER["owner_id"]}
            else:
                assert mode == "recover"
                proof = json.loads(values["terminal_proof_json"])
                assert proof["job_instance_id"] == stopped_job
                result = {"status": "SUCCEEDED", "scope": "EXPLICIT_STOPPED_WRITER_RECOVERY"}
            jobs[url] = {"status": "Completed", "exit_result": result}
            return url
        def wait_job(self, url):
            job = jobs[url]
            if job["status"] == "Failed":
                raise JobFailure(job)
            return job
        def _get(self, url):
            assert url.endswith("/livySessions/" + LIVY)
            return {"jobInstanceId": stopped_job, "state": "Failed"}

    monkeypatch.setattr("fabric_etl.phase4.ItemClient", Client)
    paths = inputs(tmp_path)
    first = run_proofs(*paths, lambda *args: None)
    terminal = (paths[3] / "terminal-writer-proof.json").read_bytes()
    second = run_proofs(*paths, lambda *args: None)
    assert first == second
    assert len(jobs) == 9
    assert terminal == (paths[3] / "terminal-writer-proof.json").read_bytes()
    assert load_json(paths[3] / "05-intentional-writer-stop.json")["status"] == "Failed"
    assert load_json(paths[3] / "acceptance.json")["status"] == "SUCCEEDED"
    journal = load_json(paths[1])
    journal["plan_id"] = "new-release-plan"
    save(paths[1], journal)
    with pytest.raises(ContractError, match="receipt differs"):
        run_proofs(*paths, lambda *args: None)
    assert len(jobs) == 9
