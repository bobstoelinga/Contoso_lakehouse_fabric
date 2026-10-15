import base64
import copy

import pytest

from fabric_etl.contracts import ContractError, fingerprint
from fabric_etl.deployment import API, DeploymentError, Response
from fabric_etl.items import ItemClient, decode_parts, deploy_items
from fabric_etl.items import normalized_settings, notebook_definition, verify_notebook
import json
from pathlib import Path


WORKSPACE = "4be37544-10b9-450d-80f0-f9b3d793b83b"
ITEM = "67751097-52f3-4582-82c2-23fd0a6c89f9"


def definition():
    return {"parts": [{"path": "Libraries/CustomLibraries/test.whl",
                       "payload": base64.b64encode(b"wheel").decode(),
                       "payloadType": "InlineBase64"}]}


def plan():
    result = {
        "status": "PLANNED_NOT_DEPLOYED", "workspace_id": WORKSPACE,
        "items": [{"id": "etl_environment", "type": "Environment",
                   "displayName": "test", "definition": definition()}],
    }
    result["plan_id"] = fingerprint(result)
    return result


class FakeClient(ItemClient):
    def __init__(self):
        super().__init__(self.transport)
        self.existing = []
        self.calls = []
        self.export = definition()

    def transport(self, method, url, body, timeout):
        self.calls.append((method, url, body))
        if method == "POST" and url.endswith("/environments"):
            self.existing.append({"id": ITEM, "displayName": "test", "type": "Environment"})
            return Response(201, {"id": ITEM}, {})
        raise AssertionError((method, url))

    def list_items(self, workspace_id):
        return self.existing

    def publish_environment(self, workspace_id, item_id):
        self.calls.append(("PUBLISH", item_id, None))
        return {"state": "Success"}

    def definition(self, _workspace_id, _item_id, item_type):
        return self.export


def test_create_verify_redeploy_and_preserve_ids(tmp_path):
    client = FakeClient()
    journal = tmp_path / "journal.json"
    first = deploy_items(client, plan(), journal)
    second = deploy_items(client, plan(), journal)
    assert first["items"]["etl_environment"]["item_id"] == second["items"]["etl_environment"]["item_id"]
    assert second["items"]["etl_environment"]["action"] == "verified"
    assert sum(c[0] == "POST" for c in client.calls) == 1
    assert journal.exists()


def test_unverified_publish_receipt_does_not_trust_previous_success(tmp_path):
    client = FakeClient()
    journal = tmp_path / "journal.json"
    result = deploy_items(client, plan(), journal)
    result["items"]["etl_environment"]["verified"] = False
    journal.write_text(json.dumps(result))
    deploy_items(client, plan(), journal)
    assert sum(call[0] == "PUBLISH" for call in client.calls) == 2
    assert sum(call[0] == "POST" for call in client.calls) == 1


def test_tamper_unowned_and_mismatched_export_fail(tmp_path):
    client = FakeClient()
    value = plan()
    value["workspace_id"] = ITEM
    with pytest.raises(ContractError):
        deploy_items(client, value, tmp_path / "journal.json")
    client.existing = [{"id": ITEM, "displayName": "test", "type": "Environment"}]
    with pytest.raises(DeploymentError, match="not owned"):
        deploy_items(client, plan(), tmp_path / "journal.json")
    client.existing = []
    client.export = copy.deepcopy(definition())
    client.export["parts"][0]["payload"] = base64.b64encode(b"wrong").decode()
    with pytest.raises(DeploymentError, match="checksum"):
        deploy_items(client, plan(), tmp_path / "journal.json")
    assert (tmp_path / "journal.json").exists()


def test_duplicate_parts_and_credentials_origin_fail_closed():
    value = definition()
    value["parts"] *= 2
    with pytest.raises(ContractError, match="Duplicate"):
        decode_parts(value)
    client = ItemClient(lambda *args: pytest.fail("Must not send"), sleep=lambda _: None)
    client.start()
    with pytest.raises(DeploymentError, match="outside"):
        client.operation(Response(202, None, {"location": "https://evil.invalid/v1/operation"}))


def test_job_failure_is_not_success():
    client = ItemClient(lambda *args: Response(200, {
        "status": "Failed", "failureReason": {"errorCode": "NotebookError"},
    }, {}))
    client.start()
    with pytest.raises(DeploymentError, match="NotebookError"):
        client.wait_job(API + "/workspaces/" + WORKSPACE + "/items/" + ITEM + "/jobs/instances/" + ITEM)


def test_server_newlines_notebook_cells_and_parameter_tags():
    root = Path(__file__).resolve().parents[1]
    source = (root / "fabric" / "items" / "verify_foundation.Notebook" / "notebook-content.py").read_text()
    definition = notebook_definition(source)
    payload = decode_parts(definition)["artifact.content.ipynb"]
    remote = json.loads(payload)
    remote["cells"][1]["source"] = [
        line.replace("\n", "\r\n") for line in remote["cells"][1]["source"]
    ]
    verify_notebook(json.dumps(remote).encode(), payload)
    remote["cells"] = []
    with pytest.raises(DeploymentError, match="inventory"):
        verify_notebook(json.dumps(remote).encode(), payload)
    remote = json.loads(payload)
    remote["cells"][0]["metadata"] = {}
    with pytest.raises(DeploymentError, match="tags"):
        verify_notebook(json.dumps(remote).encode(), payload)


def test_settings_only_normalize_documented_observed_representation():
    assert normalized_settings(b'instance_pool_id: null\nruntime_version: "2.0"\n') == normalized_settings(
        b"runtime_version: 2.0\r\r\n",
    )
    assert normalized_settings(b"driver_cores: 4") != normalized_settings(b"driver_cores: 8")


def test_regional_lro_uses_operation_id_on_trusted_origin():
    calls = []
    def transport(method, url, body, timeout):
        calls.append(url)
        if url.endswith("/result"):
            return Response(200, {"id": ITEM}, {})
        return Response(200, {"status": "Succeeded"}, {})
    client = ItemClient(transport, sleep=lambda _: None)
    client.start()
    result = client.operation(Response(202, None, {
        "location": "https://wabi-north-europe-q-primary-redirect.analysis.windows.net/v1/operations/" + ITEM,
        "x-ms-operation-id": ITEM, "retry-after": "1",
    }))
    assert result["id"] == ITEM
    assert all(url.startswith(API + "/operations/") for url in calls)


def test_update_lro_has_no_result_resource():
    calls = []
    def transport(method, url, body, timeout):
        calls.append(url)
        assert not url.endswith("/result")
        return Response(200, {"status": "Succeeded"}, {})
    client = ItemClient(transport, sleep=lambda _: None)
    client.start()
    assert client.operation(Response(202, None, {
        "location": API + "/operations/" + ITEM,
    }), require_result=False)["status"] == "Succeeded"


def test_submission_returns_before_wait_so_receipt_can_be_saved():
    calls = []
    def transport(method, url, body, timeout):
        calls.append((url, body))
        return Response(202, None, {
            "location": API + "/workspaces/" + WORKSPACE + "/items/" + ITEM + "/jobs/instances/" + ITEM,
            "retry-after": "60",
        })
    client = ItemClient(transport, sleep=lambda _: pytest.fail("Receipt must precede any wait"))
    client.start()
    url = client.submit_notebook(WORKSPACE, ITEM, {"a": "b"}, environment_id=ITEM, lakehouse_id=ITEM)
    assert url.endswith("/jobs/instances/" + ITEM)
    endpoint, body = calls[0]
    assert endpoint.endswith("/jobs/execute/instances?beta=false")
    assert body["parameters"] == [{"name": "a", "value": "b", "type": "Text"}]
    assert body["executionData"]["computeConfiguration"]["attachedEnvironment"]["itemId"] == ITEM


def test_completed_notebook_requires_explicit_feedback():
    def transport(method, url, body, timeout):
        return Response(200, {} if "beta=true" in url else {"status": "Completed"}, {})
    client = ItemClient(transport)
    client.start()
    with pytest.raises(DeploymentError, match="exit value"):
        client.wait_job(API + "/workspaces/" + WORKSPACE + "/items/" + ITEM + "/jobs/instances/" + ITEM)


@pytest.mark.parametrize("envelope", ["root", "properties"])
def test_exit_feedback_uses_documented_or_observed_job_response_field(envelope):
    exit_result = json.dumps({
        "status": "SUCCEEDED", "scope": "READ_ONLY_FOUNDATION_VISIBILITY",
    })
    def transport(method, url, body, timeout):
        if "beta=true" in url:
            response = {"exitValue": exit_result} if envelope == "root" else {
                "properties": {"exitValue": exit_result},
            }
            return Response(200, response, {})
        return Response(200, {"status": "Completed"}, {})
    client = ItemClient(transport)
    client.start()
    result = client.wait_job(API + "/workspaces/" + WORKSPACE + "/items/" + ITEM + "/jobs/instances/" + ITEM)
    assert result["exit_result"]["status"] == "SUCCEEDED"
