import base64
import hashlib
import runpy
import sys
from pathlib import Path

import pytest

from fabric_etl.contracts import ContractError


ROOT = Path(__file__).resolve().parents[1]
MAIN = runpy.run_path(str(ROOT / "deploy" / "publish-phase6-sales.py"))["main"]
OLD = "Libraries/CustomLibraries/fabric_etl_phase6-0.1.0-py3-none-any.whl"
NEW = "Libraries/CustomLibraries/fabric_etl_phase6-0.2.0-py3-none-any.whl"
GOLD = "Libraries/CustomLibraries/fabric_etl_phase6-0.3.0-py3-none-any.whl"
RECOVERY = "Libraries/CustomLibraries/fabric_etl_phase6-0.3.1-py3-none-any.whl"


def definition(parts):
    return {"parts": [
        {"path": path, "payloadType": "InlineBase64", "payload": base64.b64encode(data).decode("ascii")}
        for path, data in parts.items()
    ]}


class Client:
    def __init__(self, parts):
        self.parts = parts
        self.requests = []
        self.published = False

    def start(self):
        pass

    def assert_exclusive_setup(self, workspace):
        assert workspace == "4be37544-10b9-450d-80f0-f9b3d793b83b"

    def definition(self, *args):
        return definition(self.parts)

    def _request(self, method, url, body):
        self.requests.append((method, url))
        assert method == "POST" and url.endswith("/updateDefinition")
        self.parts = {
            part["path"]: base64.b64decode(part["payload"]) for part in body["definition"]["parts"]
        }
        self.parts[".platform"] = b"platform"
        return object()

    def operation(self, response, *, require_result):
        assert require_result is False

    def publish_environment(self, *args):
        self.published = True
        return {"state": "Succeeded"}

    def verify_published_library(self, workspace, environment, name, digest):
        assert hashlib.sha256(self.parts["Libraries/CustomLibraries/" + name]).hexdigest() == digest


def invoke(monkeypatch, tmp_path, parts, *, bundle="fabric/phase6-sales-vault-bundle.json"):
    client = Client(parts)
    monkeypatch.setitem(MAIN.__globals__, "ItemClient", lambda *args, **kwargs: client)
    fixture = original_parts()
    monkeypatch.setitem(MAIN.__globals__, "PRESERVED_SHA256", {
        key: hashlib.sha256(fixture[key]).hexdigest() for key in MAIN.__globals__["PRESERVED_SHA256"]
    })
    monkeypatch.setattr(sys, "argv", [
        "publish-phase6-sales.py", "--bundle", bundle,
        "--receipt", str(tmp_path / "publication.json"),
    ])
    return client


def original_parts():
    return {
        ".platform": b"platform",
        "Setting/Sparkcompute.yml": (ROOT / "fabric" / "items" / "etl_environment.Environment" / "Setting" / "Sparkcompute.yml").read_bytes(),
        **{
            "Libraries/CustomLibraries/" + name: (ROOT / "dist" / name).read_bytes()
            for name in (
                "fabric_etl-0.1.0-py3-none-any.whl", "fabric_etl_phase5-0.6.1-py3-none-any.whl",
                "fabric_etl_phase6-0.1.0-py3-none-any.whl",
            )
        },
    }


def test_sales_successor_preserves_base_ingestion_and_compute_and_replaces_only_pinned_predecessor(monkeypatch, tmp_path):
    parts = original_parts()
    preserved = {key: value for key, value in parts.items() if key != OLD}
    client = invoke(monkeypatch, tmp_path, parts)
    MAIN()
    assert client.published
    assert OLD not in client.parts
    assert client.parts[NEW] == (ROOT / "dist" / Path(NEW).name).read_bytes()
    assert all(client.parts[key] == value for key, value in preserved.items())
    assert len(client.requests) == 1


def test_exact_published_successor_resumes_without_republishing(monkeypatch, tmp_path):
    parts = original_parts()
    del parts[OLD]
    parts[NEW] = (ROOT / "dist" / Path(NEW).name).read_bytes()
    client = invoke(monkeypatch, tmp_path, parts)
    MAIN()
    assert not client.requests
    assert not client.published


def test_gold_successor_replaces_exact_vault_pin_without_touching_other_runtimes(monkeypatch, tmp_path):
    parts = original_parts()
    del parts[OLD]
    parts[NEW] = (ROOT / "dist" / Path(NEW).name).read_bytes()
    preserved = {key: value for key, value in parts.items() if key != NEW}
    client = invoke(monkeypatch, tmp_path, parts, bundle="fabric/phase6-sales-gold-bundle.json")
    MAIN()
    assert client.published
    assert NEW not in client.parts
    assert client.parts[GOLD] == (ROOT / "dist" / Path(GOLD).name).read_bytes()
    assert all(client.parts[key] == value for key, value in preserved.items())


def test_gold_exact_successor_resumes_without_republishing(monkeypatch, tmp_path):
    parts = original_parts()
    del parts[OLD]
    parts[GOLD] = (ROOT / "dist" / Path(GOLD).name).read_bytes()
    client = invoke(monkeypatch, tmp_path, parts, bundle="fabric/phase6-sales-gold-bundle.json")
    MAIN()
    assert not client.requests
    assert not client.published


@pytest.mark.parametrize("change", ["valid", "resume", "predecessor_drift", "wrong_predecessor", "both"])
def test_gold_patch_successor_requires_exact_published_gold_predecessor(monkeypatch, tmp_path, change):
    parts = original_parts()
    del parts[OLD]
    parts[GOLD] = (ROOT / "dist" / Path(GOLD).name).read_bytes()
    preserved = {key: value for key, value in parts.items() if key != GOLD}
    if change in {"resume", "both"}:
        parts[RECOVERY] = (ROOT / "dist" / Path(RECOVERY).name).read_bytes()
        if change == "resume":
            del parts[GOLD]
    elif change == "predecessor_drift":
        parts[GOLD] = b"wrong bytes"
    elif change == "wrong_predecessor":
        del parts[GOLD]
        parts[NEW] = (ROOT / "dist" / Path(NEW).name).read_bytes()
    client = invoke(monkeypatch, tmp_path, parts, bundle="fabric/phase6-sales-gold-recovery-bundle.json")
    if change in {"predecessor_drift", "wrong_predecessor", "both"}:
        with pytest.raises(ContractError):
            MAIN()
        assert not client.requests
    else:
        MAIN()
        assert client.published == (change == "valid")
        assert GOLD not in client.parts
        assert all(client.parts[key] == value for key, value in preserved.items())
        assert client.parts[RECOVERY] == (ROOT / "dist" / Path(RECOVERY).name).read_bytes()


def test_gold_publication_is_blocked_by_exclusive_setup_guard_before_any_write(monkeypatch, tmp_path):
    parts = original_parts()
    del parts[OLD]
    parts[NEW] = (ROOT / "dist" / Path(NEW).name).read_bytes()
    client = invoke(monkeypatch, tmp_path, parts, bundle="fabric/phase6-sales-gold-bundle.json")

    def busy(workspace):
        raise ContractError("Fixture active job prevents exclusive publication")

    monkeypatch.setattr(client, "assert_exclusive_setup", busy)
    with pytest.raises(ContractError, match="active job"):
        MAIN()
    assert not client.requests
    assert not client.published


@pytest.mark.parametrize("change", ["predecessor", "settings", "extra", "missing", "both", "successor"])
def test_sales_publication_drift_fails_before_remote_mutation(monkeypatch, tmp_path, change):
    parts = original_parts()
    if change == "predecessor":
        parts[OLD] = b"wrong predecessor"
    elif change == "settings":
        parts["Setting/Sparkcompute.yml"] = b"wrong settings"
    elif change == "extra":
        parts["Libraries/CustomLibraries/foreign.whl"] = b"foreign"
    elif change == "missing":
        del parts[OLD]
    elif change == "both":
        parts[NEW] = (ROOT / "dist" / Path(NEW).name).read_bytes()
    else:
        del parts[OLD]
        parts[NEW] = b"wrong successor"
    client = invoke(monkeypatch, tmp_path, parts)
    with pytest.raises(ContractError):
        MAIN()
    assert not client.requests
