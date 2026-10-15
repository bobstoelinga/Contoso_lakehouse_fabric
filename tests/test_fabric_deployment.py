from __future__ import annotations

from uuid import UUID

import pytest

from fabric_etl.contracts import Environment
from fabric_etl.deployment import API, DeploymentError, FoundationDeployer, Response, api_url
from fabric_etl.__main__ import AzureCliTransport, NoRedirect, main


WORKSPACE = str(UUID(int=1))
IDS = {"control": str(UUID(int=2)), "processing": str(UUID(int=3)), "gold": str(UUID(int=4))}
ENDPOINT = str(UUID(int=5))


def environment(pinned=False):
    return Environment.from_dict({
        "contract_version": "1.1", "project_id": "test", "environment": "dev",
        "tenant_id": str(UUID(int=10)),
        "workspace_id": WORKSPACE, "workspace_name": "test_workspace",
        "lakehouses": {
            role: {"name": f"lh_{role}", "item_id": item_id if pinned else None}
            for role, item_id in IDS.items()
        },
    })


def item(role, **properties):
    return {
        "id": IDS[role], "displayName": f"lh_{role}",
        "type": "Lakehouse", "workspaceId": WORKSPACE,
        "properties": {
            "defaultSchema": "dbo",
            "sqlEndpointProperties": {"id": ENDPOINT, "provisioningStatus": "Success"},
            **properties,
        },
    }


class FakeFabric:
    def __init__(self, existing=True):
        self.existing = [item(role) for role in IDS] if existing else []
        self.calls = []

    def __call__(self, method, url, body, timeout):
        self.calls.append((method, url, body))
        assert 0 < timeout <= 60
        if method == "POST":
            assert body["creationPayload"] == {"enableSchemas": True}
            role = body["displayName"].removeprefix("lh_")
            created = item(role)
            self.existing.append(created)
            return Response(201, created, {})
        if url.endswith("/lakehouses"):
            return Response(200, {"value": self.existing}, {})
        matches = [entry for entry in self.existing if url.endswith("/" + entry["id"])]
        assert len(matches) == 1
        return Response(200, matches[0], {})


def test_two_foundation_deploys_preserve_ids_and_never_delete():
    fabric = FakeFabric(existing=False)
    deployer = FoundationDeployer(fabric)
    first = deployer.reconcile(environment())
    second = deployer.reconcile(environment())
    assert [row["item_id"] for row in first] == [row["item_id"] for row in second]
    assert all(row["action"] == "created" for row in first)
    assert all(row["action"] == "reused" for row in second)
    assert [method for method, _, _ in fabric.calls].count("POST") == 3
    assert all(method in {"POST", "GET"} for method, _, _ in fabric.calls)


def test_pinned_binding_is_verified_not_recreated():
    fabric = FakeFabric()
    assert all(row["action"] == "verified" for row in FoundationDeployer(fabric).reconcile(environment(True)))
    assert not any(method == "POST" for method, _, _ in fabric.calls)


@pytest.mark.parametrize("change", ["schema", "workspace", "name", "endpoint", "ambiguous", "pinned"])
def test_existing_resource_conflicts_fail_instead_of_silent_adoption(change):
    fabric = FakeFabric()
    env = environment(change == "pinned")
    if change == "schema":
        fabric.existing[0]["properties"]["defaultSchema"] = None
    elif change == "workspace":
        fabric.existing[0]["workspaceId"] = str(UUID(int=99))
    elif change == "name":
        fabric.existing[0]["displayName"] = "LH_CONTROL"
    elif change == "endpoint":
        fabric.existing[0]["properties"]["sqlEndpointProperties"]["provisioningStatus"] = "Failed"
    elif change == "ambiguous":
        fabric.existing.append(item("control"))
    else:
        fabric.existing[0]["id"] = str(UUID(int=99))
    with pytest.raises(DeploymentError):
        FoundationDeployer(fabric).reconcile(env)
    assert not any(method == "POST" for method, _, _ in fabric.calls)


def test_unlicensed_identity_fails_without_mutation():
    calls = []

    def transport(method, url, body, timeout):
        calls.append(method)
        return Response(401, {"errorCode": "UserNotLicensed"}, {})

    with pytest.raises(DeploymentError, match="UserNotLicensed"):
        FoundationDeployer(transport).reconcile(environment())
    assert calls == ["GET"]


def test_post_is_never_retried_after_uncertain_server_failure():
    calls = []

    def transport(method, url, body, timeout):
        calls.append(method)
        if method == "GET":
            return Response(200, {"value": []}, {})
        return Response(503, {"errorCode": "Unavailable"}, {})

    with pytest.raises(DeploymentError):
        FoundationDeployer(transport).reconcile(environment())
    assert calls == ["GET", "POST"]


def test_long_running_create_polls_then_validates_result():
    fabric = FakeFabric()
    fabric.existing = [item("processing"), item("gold")]
    operation = f"{API}/operations/{UUID(int=6)}"
    states = iter(["Running", "Succeeded"])
    calls = []

    def transport(method, url, body, timeout):
        calls.append((method, url))
        if method == "POST":
            return Response(202, None, {"location": operation, "retry-after": "1"})
        if url == operation:
            return Response(200, {"status": next(states)}, {"retry-after": "1"})
        if url == operation + "/result":
            fabric.existing.append(item("control"))
            return Response(200, item("control"), {})
        return fabric(method, url, body, timeout)

    outcomes = FoundationDeployer(transport, sleep=lambda delay: None).reconcile(environment())
    assert outcomes[0]["action"] == "created"
    assert calls.count(("POST", f"{API}/workspaces/{WORKSPACE}/lakehouses")) == 1
    assert calls.count(("GET", operation)) == 2


def test_read_retry_honors_retry_after_and_deadline():
    now = [0.0]
    delays = []

    def sleep(delay):
        delays.append(delay)
        now[0] += delay

    def transport(method, url, body, timeout):
        return Response(429, {"errorCode": "Throttled"}, {"retry-after": "3"})

    with pytest.raises(DeploymentError, match="deadline"):
        FoundationDeployer(
            transport, timeout_seconds=5, clock=lambda: now[0], sleep=sleep,
        ).reconcile(environment())
    assert delays == [3, 2]


@pytest.mark.parametrize("url", [
    "https://evil.example/v1/operation", "http://api.fabric.microsoft.com/v1/items",
    "https://api.fabric.microsoft.com.evil.example/v1/items",
    "https://api.fabric.microsoft.com/not-v1/items",
])
def test_untrusted_operation_urls_are_rejected(url):
    with pytest.raises(DeploymentError):
        api_url(url)


def test_cli_invalid_json_has_failure_exit_code_and_explicit_error(tmp_path, capsys):
    path = tmp_path / "duplicate.json"
    path.write_text('{"x":1,"x":2}')
    assert main(["validate-json", str(path)]) == 1
    output = capsys.readouterr()
    assert "Duplicate JSON field" in output.err
    assert not output.out


@pytest.mark.parametrize("timeout", [0, -1, float("nan"), float("inf")])
def test_invalid_deadline_is_rejected(timeout):
    with pytest.raises(ValueError, match="positive"):
        FoundationDeployer(FakeFabric(), timeout_seconds=timeout)


def test_transport_rejects_untrusted_origin_before_acquiring_token(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("Credential lookup must not run for an untrusted URL")

    monkeypatch.setattr("fabric_etl.__main__.shutil.which", forbidden)
    with pytest.raises(DeploymentError):
        AzureCliTransport(str(UUID(int=10)))("GET", "https://evil.example/v1/items", None, 60)


def test_authorized_http_redirect_is_not_followed():
    assert NoRedirect().redirect_request(
        None, None, 302, "Found", {}, "https://evil.example",
    ) is None


def test_transport_pins_token_acquisition_to_configured_tenant(monkeypatch):
    import subprocess

    tenant = str(UUID(int=10))
    commands = []

    def acquire(command, **kwargs):
        commands.append(command)
        return subprocess.CompletedProcess(command, 1, "", "authentication required")

    monkeypatch.setattr("fabric_etl.__main__.shutil.which", lambda name: "az")
    monkeypatch.setattr("fabric_etl.__main__.subprocess.run", acquire)
    with pytest.raises(DeploymentError, match="token acquisition failed"):
        AzureCliTransport(tenant)("GET", f"{API}/workspaces", None, 60)
    assert commands == [[
        "az", "account", "get-access-token", "--tenant", tenant,
        "--resource", "https://api.fabric.microsoft.com", "--output", "json",
    ]]
