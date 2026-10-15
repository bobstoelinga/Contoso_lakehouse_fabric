from __future__ import annotations

import base64
import json
import shutil
import zipfile
from pathlib import Path

import pytest

from fabric_etl.contracts import ContractError, load_json
from fabric_etl.plan import build_plan
from fabric_etl.runtime import verify_foundation


ROOT = Path(__file__).resolve().parents[1]


def wheel(path, *, version="0.1.0", changed=False, legacy=False):
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("fabric_etl-0.1.0.dist-info/METADATA",
                         f"Metadata-Version: 2.1\r\nName: fabric-etl\r\nVersion: {version}\r\n")
        for source in (ROOT / "src" / "fabric_etl").glob("*.py"):
            data = source.read_bytes()
            if changed and source.name == "runtime.py":
                data += b"\n# changed\n"
            archive.writestr("fabric_etl/" + source.name, data)
        if legacy:
            archive.writestr("contoso_lakehouse/__init__.py", "")
    return path


def plan(wheel_path, repo=ROOT):
    return build_plan(repo, ROOT / "projects" / "contoso" / "release.json",
                      ROOT / "environments" / "contoso.dev.json", wheel_path)


def test_local_plan_is_repeatable_and_not_a_runtime_success(tmp_path):
    whl = wheel(tmp_path / "fabric_etl-0.1.0-py3-none-any.whl")
    first = plan(whl)
    assert plan(whl) == first
    assert first["status"] == "PLANNED_NOT_DEPLOYED"
    assert first["scope"] == "PHASE3_FOUNDATION_PREPARATION"
    assert first["runtime_blockers"] and first["pending_runtime_stages"]
    assert not first["delete_items"] and not first["replay_data"]
    assert first["unresolved_bindings"] == ["etl_environment_item_id"]
    assert [item["type"] for item in first["items"]] == ["Environment", "Notebook", "Notebook", "Notebook"]
    assert first["items"][1]["requires"] == ["etl_environment"]
    definition = first["items"][1]["definition"]
    source = base64.b64decode(definition["parts"][0]["payload"]).decode()
    assert "${etl_environment_item_id}" in source
    assert '"environmentId"' in source
    assert first["workspace_id"] in source
    assert "{control}" not in first["migrations"][0]["rendered_sql"]
    assert "`lh_contoso_control`.metadata" in first["migrations"][0]["rendered_sql"]
    assert first["migrations"][1]["requires"] == ["001_namespaces"]
    ddl = first["migrations"][1]["rendered_sql"]
    assert ddl.count("CREATE TABLE IF NOT EXISTS") == 11
    assert "INSERT " not in ddl and "MERGE " not in ddl and "DROP " not in ddl


@pytest.mark.parametrize("kwargs", [
    {"version": "9.0"}, {"changed": True}, {"legacy": True},
])
def test_plan_rejects_wrong_stale_or_legacy_wheel(tmp_path, kwargs):
    with pytest.raises(ContractError):
        plan(wheel(tmp_path / "test.whl", **kwargs))


def test_plan_rejects_unknown_definition_or_missing_prerequisite(tmp_path):
    repo = tmp_path / "repo"
    for folder in ("fabric", "sql", "src"):
        shutil.copytree(ROOT / folder, repo / folder)
    path = repo / "fabric" / "deployment.json"
    data = load_json(path)
    data["items"][0]["type"] = "Unsupported"
    path.write_text(json.dumps(data))
    with pytest.raises(ContractError, match="item type"):
        plan(wheel(tmp_path / "test.whl"), repo)
    data = load_json(ROOT / "fabric" / "deployment.json")
    data["items"].reverse()
    path.write_text(json.dumps(data))
    with pytest.raises(ContractError, match="prerequisites"):
        plan(wheel(tmp_path / "test.whl"), repo)


def test_plan_rejects_missing_notebook_dependency_header(tmp_path):
    repo = tmp_path / "repo"
    for folder in ("fabric", "sql", "src"):
        shutil.copytree(ROOT / folder, repo / folder)
    notebook = repo / "fabric" / "items" / "verify_foundation.Notebook" / "notebook-content.py"
    notebook.write_text(notebook.read_text().replace('"dependencies": {}', '"dependencies": {"unexpected": 1}'))
    with pytest.raises(ContractError, match="dependency header"):
        plan(wheel(tmp_path / "test.whl"), repo)


class RecordingSpark:
    def __init__(self, empty=False):
        self.queries = []
        self.empty = empty

    def sql(self, query):
        self.queries.append(query)
        return self

    def collect(self):
        return [] if self.empty else [("dbo",)]


def test_runtime_verification_only_reads_three_explicit_namespaces():
    spark = RecordingSpark()
    env = (ROOT / "environments" / "contoso.dev.json").read_text()
    result = verify_foundation(spark, env, "0.1.0")
    assert result["scope"] == "READ_ONLY_FOUNDATION_VISIBILITY"
    assert len(spark.queries) == 3
    assert all(q.startswith("SHOW TABLES IN ") and q.endswith(".`dbo`") for q in spark.queries)
    assert "`Contoso_lakehouse_ontwikkel`.`lh_contoso_gold`" in spark.queries[-1]


def test_runtime_version_missing_schema_and_duplicate_json_fail_closed():
    env = (ROOT / "environments" / "contoso.dev.json").read_text()
    with pytest.raises(ContractError, match="version"):
        verify_foundation(RecordingSpark(), env, "9.0")
    assert verify_foundation(RecordingSpark(True), env, "0.1.0")["status"] == "SUCCEEDED"
    with pytest.raises(ContractError, match="Duplicate"):
        verify_foundation(RecordingSpark(), '{"project_id":"a","project_id":"b"}', "0.1.0")
