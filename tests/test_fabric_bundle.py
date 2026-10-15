from __future__ import annotations

import ast
import base64
import hashlib
import json
import shutil
import zipfile
from pathlib import Path

import pytest

from fabric_etl.bundle import build_phase5_plan, deploy_phase5_plan
from fabric_etl.contracts import ContractError, fingerprint, load_json
from fabric_etl.deployment import DeploymentError, Response
from fabric_etl.items import ItemClient
from fabric_etl_phase5.ingestion import RAW_IDENTITY_FIELDS


ROOT = Path(__file__).resolve().parents[1]
WORKSPACE = "4be37544-10b9-450d-80f0-f9b3d793b83b"
ENVIRONMENT_ID = "7266a872-dd91-4aa6-a0b4-d4cc5b862643"
NOTEBOOK_ID = "f16f1d59-52f4-46ab-978b-8854b3520207"
LIBRARY_NAME = "fabric_etl_phase5-0.1.0-py3-none-any.whl"
BRONZE_NOTEBOOK_ID = "d7adac55-455a-4f0b-88c5-a99bd92b4551"


def test_sales_vault_bundle_pins_successor_and_exact_committed_quality_input_without_full_phase_claim():
    plan = build_phase5_plan(ROOT, ROOT / "fabric" / "phase6-sales-vault-bundle.json")
    assert plan["scope"] == "PHASE6_SALES_VAULT_PROOF"
    assert plan["environment_library"]["name"] == "fabric_etl_phase6-0.2.0-py3-none-any.whl"
    source = base64.b64decode(plan["items"][0]["definition"]["parts"][0]["payload"]).decode()
    ast.parse(source)
    assert 'quality_receipt_json = "{}"' not in source
    assert 'project_json = "{}"' not in source
    assert 'environment_json = "{}"' not in source
    assert 'expected_phase6_version = "0.2.0"' in source
    assert '"phase6_complete": False' in source
    assert "be2008c7-5d85-5692-ade3-3eb78a39d457" in source
    assert "read_vault_commit(" in source
    assert "expected_vault_rows(number)" in source


def prepare_gold_bundle(tmp_path):
    repository, _, _ = prepare_bundle(tmp_path)
    shutil.copytree(ROOT / "projects" / "contoso", repository / "projects" / "contoso")
    source = repository / "fabric" / "items" / "phase6_sales_gold.Notebook" / "notebook-content.py"
    source.parent.mkdir(parents=True)
    source.write_bytes((ROOT / "fabric" / "items" / "phase6_sales_gold.Notebook" / "notebook-content.py").read_bytes())
    wheel = repository / "dist" / "fabric_etl_phase6-0.3.0-py3-none-any.whl"
    with zipfile.ZipFile(wheel, "w") as archive:
        archive.writestr("fabric_etl_phase6-0.3.0.dist-info/METADATA", "Name: fabric-etl-phase6\nVersion: 0.3.0\n")
        for module in ("__init__", "quality", "vault", "proof", "pit", "gold", "publication"):
            archive.writestr("fabric_etl_phase6/" + module + ".py", "")
    manifest = load_json(ROOT / "fabric" / "phase6-sales-vault-bundle.json")
    manifest.update(bundle_id="contoso_phase6_sales_gold", scope="PHASE6_SALES_GOLD_PROOF")
    manifest["library"].update(path="dist/" + wheel.name, version="0.3.0",
                               sha256=hashlib.sha256(wheel.read_bytes()).hexdigest())
    manifest["notebooks"] = [{
        "id": "phase6_sales_gold", "display_name": "{project_id}_phase6_sales_gold_{environment}",
        "source": "fabric/items/phase6_sales_gold.Notebook/notebook-content.py",
    }]
    bundle = repository / "fabric" / "phase6-sales-gold-bundle.json"
    bundle.write_text(json.dumps(manifest))
    from fabric_etl.project import ProjectRelease

    release = ProjectRelease.load(repository / "projects" / "contoso" / "release.json")
    receipt_path = repository / "deploy" / "evidence" / "phase6-sales-vault-v1-receipt.json"
    receipt_path.parent.mkdir(parents=True)
    receipt_path.write_text(json.dumps({
        "status": "COMPLETED", "job": {"status": "Completed", "exit_result": {
            "scope": "PHASE6_SALES_COMMITTED_VAULT", "status": "SUCCEEDED",
            "release_id": release.release_id, "fixture_marker": "unit-test-input-only",
        }},
    }))
    return repository, bundle, receipt_path


def test_gold_bundle_embeds_successful_vault_receipt_and_original_checksums_without_full_phase_claim(tmp_path):
    repository, bundle, _ = prepare_gold_bundle(tmp_path)
    plan = build_phase5_plan(repository, bundle)
    assert plan["scope"] == "PHASE6_SALES_GOLD_PROOF"
    source = base64.b64decode(plan["items"][0]["definition"]["parts"][0]["payload"]).decode()
    ast.parse(source)
    for parameter in ("environment_json", "project_json", "vault_receipt_json", "gold_sources_json"):
        assert parameter + ' = "{}"' not in source
    assert "unit-test-input-only" in source
    assert 'expected_phase6_version = "0.3.0"' in source
    assert '"phase6_complete": False' in source
    assert "expected_gold_rows(number, load_dates)" in source
    assert "active_pointer_unchanged" in source
    assert 'spark.sql("CREATE SCHEMA IF NOT EXISTS " + current_schema)' in source
    assert "GH_FCT_SALES" in source


def test_gold_recovery_manifest_query_uses_only_registered_commit_columns():
    from fabric_etl.control import literal
    from fabric_etl_phase6.publication import PRODUCT_ID

    source = ROOT / "fabric" / "items" / "phase6_sales_gold.Notebook" / "notebook-content.py"
    tree = ast.parse(source.read_text())
    predicate = next(node.value for node in ast.walk(tree) if isinstance(node, ast.Assign)
                     and any(isinstance(target, ast.Name) and target.id == "predicate" for target in node.targets))
    query = eval(compile(ast.Expression(predicate), str(source), "eval"), {
        "literal": literal, "batch_id": "test-batch", "PRODUCT_ID": PRODUCT_ID,
    })
    assert query == (
        f"batch_id = {literal('test-batch')} AND layer = 'GOLD' "
        f"AND entity_id = {literal(PRODUCT_ID)} AND status = 'COMMITTED'"
    )
    assert "project_id" not in query


@pytest.mark.parametrize("change", ["pending", "failed", "scope", "release", "sql", "parameter"])
def test_gold_bundle_rejects_uncompleted_foreign_or_unpinned_inputs(tmp_path, change):
    repository, bundle, receipt_path = prepare_gold_bundle(tmp_path)
    receipt = load_json(receipt_path)
    if change == "pending":
        receipt["status"] = "SUBMITTED"
    elif change == "failed":
        receipt["job"]["status"] = "Failed"
    elif change in {"scope", "release"}:
        receipt["job"]["exit_result"]["scope" if change == "scope" else "release_id"] = "foreign"
    elif change == "sql":
        sql = repository / "projects" / "contoso" / "sql" / "GH_FCT_SALES.sql"
        sql.write_bytes(sql.read_bytes() + b" ")
    else:
        source = repository / "fabric" / "items" / "phase6_sales_gold.Notebook" / "notebook-content.py"
        source.write_text(source.read_text().replace('vault_receipt_json = "{}"', "vault_receipt_json = None"))
    receipt_path.write_text(json.dumps(receipt))
    with pytest.raises(ContractError):
        build_phase5_plan(repository, bundle)


def prepare_bundle(tmp_path: Path) -> tuple[Path, Path, bytes]:
    tmp_path.mkdir(parents=True, exist_ok=True)
    (tmp_path / "environments").mkdir()
    (tmp_path / "fabric" / "items" / "phase5_runtime_probe.Notebook").mkdir(parents=True)
    (tmp_path / "dist").mkdir()
    binding = load_json(ROOT / "environments" / "contoso.dev.json")
    (tmp_path / "environments" / "contoso.dev.json").write_text(json.dumps(binding))
    source = ROOT / "fabric" / "items" / "phase5_runtime_probe.Notebook" / "notebook-content.py"
    source_path = tmp_path / "fabric" / "items" / "phase5_runtime_probe.Notebook" / "notebook-content.py"
    source_path.write_bytes(source.read_bytes())

    wheel = tmp_path / "dist" / LIBRARY_NAME
    with zipfile.ZipFile(wheel, "w") as archive:
        archive.writestr("fabric_etl_phase5-0.1.0.dist-info/METADATA", "Name: fabric-etl-phase5\nVersion: 0.1.0\n")
        archive.writestr("fabric_etl_phase5/__init__.py", "__version__ = '0.1.0'\n")
        archive.writestr("fabric_etl_phase5/ingestion.py", "RAW_IDENTITY_FIELDS = ()\n")
    wheel_bytes = wheel.read_bytes()
    manifest = load_json(ROOT / "fabric" / "phase5-runtime-bundle.json")
    manifest["environment_binding"]["path"] = "environments/contoso.dev.json"
    manifest["library"]["path"] = "dist/" + LIBRARY_NAME
    manifest["library"]["sha256"] = hashlib.sha256(wheel_bytes).hexdigest()
    (tmp_path / "fabric" / "phase5-runtime-bundle.json").write_text(json.dumps(manifest))
    return tmp_path, tmp_path / "fabric" / "phase5-runtime-bundle.json", wheel_bytes


def prepare_bronze_bundle(tmp_path: Path) -> tuple[Path, Path, bytes]:
    repository, _, wheel_bytes = prepare_bundle(tmp_path)
    source = ROOT / "fabric" / "items" / "phase5_bronze_probe.Notebook" / "notebook-content.py"
    source_path = repository / "fabric" / "items" / "phase5_bronze_probe.Notebook" / "notebook-content.py"
    source_path.parent.mkdir(parents=True)
    source_path.write_bytes(source.read_bytes())
    manifest = load_json(ROOT / "fabric" / "phase5-bronze-bundle.json")
    manifest["environment_binding"]["path"] = "environments/contoso.dev.json"
    manifest["library"]["path"] = "dist/" + LIBRARY_NAME
    manifest["library"]["sha256"] = hashlib.sha256(wheel_bytes).hexdigest()
    bundle = repository / "fabric" / "phase5-bronze-bundle.json"
    bundle.write_text(json.dumps(manifest))
    return repository, bundle, wheel_bytes


def test_window_bundle_preserves_new_version_pin_and_environment_binding():
    plan = build_phase5_plan(ROOT, ROOT / "fabric" / "phase5-quality-window-bundle.json")
    assert plan["scope"] == "PHASE5_QUALITY_WINDOW_PROBE"
    assert plan["environment_library"]["name"] == "fabric_etl_phase5-0.3.0-py3-none-any.whl"
    encoded = plan["items"][0]["definition"]["parts"][0]["payload"]
    source = base64.b64decode(encoded).decode()
    ast.parse(source)
    assert 'environment_json = "{}"' not in source
    assert 'expected_phase5_version = "0.3.0"' in source
    assert "_output_fingerprint(bronze)" in source
    assert 'frame_fingerprint(bronze, "raw_record_id")' not in source


def test_actual_metadata_bundle_pins_candidate_and_new_runtime_without_activating():
    plan = build_phase5_plan(ROOT, ROOT / "fabric" / "phase5-metadata-quality-bundle.json")
    assert plan["scope"] == "PHASE5_METADATA_QUALITY_PROBE"
    assert plan["environment_library"]["name"] == "fabric_etl_phase5-0.4.0-py3-none-any.whl"
    source = base64.b64decode(plan["items"][0]["definition"]["parts"][0]["payload"]).decode()
    ast.parse(source)
    assert 'project_json = "{}"' not in source
    assert 'environment_json = "{}"' not in source
    assert "c9beb08ca216ac512739c00a6dde127190bd7e53713327080abe59a10b5406f4" in source
    assert 'expected_phase5_version = "0.4.0"' in source


def test_metadata_writer_bundle_is_isolated_version_pinned_and_uses_shared_writer():
    plan = build_phase5_plan(ROOT, ROOT / "fabric" / "phase5-metadata-write-bundle.json")
    assert plan["scope"] == "PHASE5_METADATA_WRITE_PROBE"
    assert plan["environment_library"]["name"] == "fabric_etl_phase5-0.5.0-py3-none-any.whl"
    source = base64.b64decode(plan["items"][0]["definition"]["parts"][0]["payload"]).decode()
    ast.parse(source)
    assert 'project_json = "{}"' not in source
    assert 'environment_json = "{}"' not in source
    assert 'expected_phase5_version = "0.5.0"' in source
    assert "write_metadata_quality(" in source
    assert "proof_only=True" in source
    assert "read_manifest_records(" in source
    assert "PHASE4_PROOF" in source


def test_exit_bundle_binds_shared_delivery_runtime_and_preserves_old_pins():
    plan = build_phase5_plan(ROOT, ROOT / "fabric" / "phase5-exit-bundle.json")
    assert plan["scope"] == "PHASE5_EXIT_PROBE"
    assert plan["environment_library"]["name"] == "fabric_etl_phase5-0.6.1-py3-none-any.whl"
    source = base64.b64decode(plan["items"][0]["definition"]["parts"][0]["payload"]).decode()
    ast.parse(source)
    assert 'environment_json = "{}"' not in source
    assert "ingest_registered_delivery(" in source
    assert "verify_onelake_files(" in source
    assert '["JSON", "CSV", "PARQUET"]' in source
    assert "InterruptedDelivery" in source


def test_phase6_quality_bundle_pins_separate_library_and_committed_references():
    plan = build_phase5_plan(ROOT, ROOT / "fabric" / "phase6-sales-quality-bundle.json")
    assert plan["scope"] == "PHASE6_SALES_QUALITY_PROOF"
    assert plan["environment_library"]["name"] == "fabric_etl_phase6-0.1.0-py3-none-any.whl"
    source = base64.b64decode(plan["items"][0]["definition"]["parts"][0]["payload"]).decode()
    ast.parse(source)
    assert 'project_json = "{}"' not in source
    assert 'environment_json = "{}"' not in source
    assert "write_quality(" in source
    assert "reference_commits=" in source
    assert "expected_output_verified" in source
    assert '"phase6_complete": False' in source


def test_sales_source_bundle_is_schema_only_and_pins_native_runtime():
    plan = build_phase5_plan(ROOT, ROOT / "fabric" / "phase5-sales-source-bundle.json")
    assert plan["scope"] == "PHASE5_SALES_SOURCE_PROBE"
    assert plan["environment_library"]["name"] == "fabric_etl_phase5-0.3.0-py3-none-any.whl"
    source = base64.b64decode(plan["items"][0]["definition"]["parts"][0]["payload"]).decode()
    ast.parse(source)
    assert 'environment_json = "{}"' not in source
    assert "SELECT TOP (0) * FROM SalesLT.vw_databricks_sales_order_line" in source
    assert ".mssql()" in source
    assert "accessToken" not in source


class FakeBundleClient(ItemClient):
    def __init__(
        self, library_bytes: bytes, *,
        notebook_id: str = NOTEBOOK_ID,
        display_name: str = "contoso_phase5_runtime_probe_dev",
        library_name: str = LIBRARY_NAME,
    ):
        super().__init__(self.transport, sleep=lambda _: None)
        self.library_bytes = library_bytes
        self.notebook_id = notebook_id
        self.display_name = display_name
        self.library_name = library_name
        self.items: list[dict] = []
        self.created_definition: dict | None = None
        self.create_count = 0
        self.library_checks = 0

    def transport(self, method, url, body, timeout):
        if method == "POST" and url.endswith("/notebooks"):
            self.create_count += 1
            self.created_definition = body["definition"]
            self.items.append({
                "id": self.notebook_id,
                "displayName": self.display_name,
                "type": "Notebook",
            })
            return Response(201, {"id": self.notebook_id}, {})
        raise AssertionError((method, url))

    def list_items(self, workspace_id):
        return self.items

    def verify_published_library(self, workspace_id, environment_id, library_name, expected_sha256):
        self.library_checks += 1
        assert workspace_id == WORKSPACE
        assert environment_id == ENVIRONMENT_ID
        assert library_name == self.library_name
        assert hashlib.sha256(self.library_bytes).hexdigest() == expected_sha256

    def definition(self, workspace_id, item_id, item_type):
        if item_type == "Environment":
            return {"parts": [{
                "path": f"Libraries/CustomLibraries/{LIBRARY_NAME}",
                "payloadType": "InlineBase64",
                "payload": base64.b64encode(self.library_bytes).decode(),
            }]}
        assert self.created_definition is not None
        notebook_part = self.created_definition["parts"][0]
        return {"parts": [{
            "path": "notebook-content.ipynb",
            "payloadType": notebook_part["payloadType"],
            "payload": notebook_part["payload"],
        }]}


def test_phase5_bundle_plan_pins_published_environment_wheel_and_notebook(tmp_path):
    repository, bundle, wheel_bytes = prepare_bundle(tmp_path)
    plan = build_phase5_plan(repository, bundle)
    assert plan["scope"] == "PHASE5_RUNTIME_PROBE"
    assert plan["runtime_blockers"] == ["QUALITY_REJECT_VAULT_GOLD_AND_SECURITY_ACCEPTANCE"]
    assert plan["environment_binding"] == {
        "item_id": ENVIRONMENT_ID, "workspace_id": WORKSPACE,
    }
    assert plan["environment_library"]["sha256"] == hashlib.sha256(wheel_bytes).hexdigest()
    assert plan["plan_id"] == fingerprint({key: value for key, value in plan.items() if key != "plan_id"})
    source = base64.b64decode(plan["items"][0]["definition"]["parts"][0]["payload"]).decode()
    assert f'"environmentId": "${{etl_environment_item_id}}"' in source


def test_runtime_probe_spark_identity_matches_the_raw_identity_contract():
    source_path = ROOT / "fabric" / "items" / "phase5_runtime_probe.Notebook" / "notebook-content.py"
    module = ast.parse(source_path.read_text(encoding="utf-8"))
    identity_assignment = next(
        node for node in module.body
        if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == "identity" for target in node.targets)
    )
    identity_dict = identity_assignment.value
    assert isinstance(identity_dict, ast.Dict)
    identity_fields = {
        key.value for key in identity_dict.keys
        if isinstance(key, ast.Constant) and isinstance(key.value, str)
    }
    assert identity_fields == set(RAW_IDENTITY_FIELDS)


def test_quality_bundle_requires_versioned_runtime_and_exact_environment_binding(tmp_path):
    repository, _, _ = prepare_bundle(tmp_path)
    wheel = repository / "dist" / "fabric_etl_phase5-0.2.0-py3-none-any.whl"
    with zipfile.ZipFile(wheel, "w") as archive:
        archive.writestr("fabric_etl_phase5-0.2.0.dist-info/METADATA", "Name: fabric-etl-phase5\nVersion: 0.2.0\n")
        for name in ("__init__.py", "ingestion.py", "quality.py"):
            archive.writestr("fabric_etl_phase5/" + name, (ROOT / "src" / "fabric_etl_phase5" / name).read_bytes())
    source_path = repository / "fabric" / "items" / "phase5_quality_probe.Notebook" / "notebook-content.py"
    source_path.parent.mkdir(parents=True)
    source_path.write_bytes((ROOT / "fabric" / "items" / "phase5_quality_probe.Notebook" / "notebook-content.py").read_bytes())
    manifest = load_json(ROOT / "fabric" / "phase5-quality-bundle.json")
    manifest["library"]["sha256"] = hashlib.sha256(wheel.read_bytes()).hexdigest()
    bundle = repository / "fabric" / "phase5-quality-bundle.json"
    bundle.write_text(json.dumps(manifest))
    plan = build_phase5_plan(repository, bundle)
    assert plan["scope"] == "PHASE5_QUALITY_PROBE"
    source = base64.b64decode(plan["items"][0]["definition"]["parts"][0]["payload"]).decode()
    module = ast.parse(source)
    assignments = {
        target.id: node.value for node in module.body if isinstance(node, ast.Assign)
        for target in node.targets if isinstance(target, ast.Name)
    }
    assert json.loads(ast.literal_eval(assignments["environment_json"])) == load_json(repository / "environments" / "contoso.dev.json")
    raw = ast.literal_eval(assignments["RAW_TEXT"]).encode()
    assert len(raw) == 368
    assert hashlib.sha256(raw).hexdigest() == ast.literal_eval(assignments["RAW_SHA256"])
    client = FakeBundleClient(
        wheel.read_bytes(), display_name="contoso_phase5_quality_probe_dev",
        library_name=wheel.name,
    )
    journal = tmp_path / "quality-journal.json"
    result = deploy_phase5_plan(client, plan, journal)
    assert result["items"]["phase5_quality_probe"]["verified"]
    deploy_phase5_plan(client, plan, journal)
    assert client.create_count == 1
    manifest["library"]["version"] = "0.1.0"
    bundle.write_text(json.dumps(manifest))
    with pytest.raises(ContractError, match="library identity"):
        build_phase5_plan(repository, bundle)


def test_chain_bundle_pins_three_deliveries_and_preserves_soft_delete_history(tmp_path):
    repository, _, _ = prepare_bundle(tmp_path)
    wheel = repository / "dist" / "fabric_etl_phase5-0.2.0-py3-none-any.whl"
    with zipfile.ZipFile(wheel, "w") as archive:
        archive.writestr("fabric_etl_phase5-0.2.0.dist-info/METADATA", "Name: fabric-etl-phase5\nVersion: 0.2.0\n")
        for name in ("__init__.py", "ingestion.py", "quality.py"):
            archive.writestr("fabric_etl_phase5/" + name, (ROOT / "src" / "fabric_etl_phase5" / name).read_bytes())
    notebook = ROOT / "fabric" / "items" / "phase5_chain_probe.Notebook" / "notebook-content.py"
    local = repository / "fabric" / "items" / "phase5_chain_probe.Notebook" / "notebook-content.py"
    local.parent.mkdir(parents=True)
    local.write_bytes(notebook.read_bytes())
    manifest = load_json(ROOT / "fabric" / "phase5-chain-bundle.json")
    manifest["library"]["sha256"] = hashlib.sha256(wheel.read_bytes()).hexdigest()
    bundle = repository / "fabric" / "phase5-chain-bundle.json"
    bundle.write_text(json.dumps(manifest))
    plan = build_phase5_plan(repository, bundle)
    assert plan["scope"] == "PHASE5_CHAIN_PROBE"
    source = base64.b64decode(plan["items"][0]["definition"]["parts"][0]["payload"]).decode()
    module = ast.parse(source)
    fixtures_node = next(
        node.value for node in module.body if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == "FIXTURES" for target in node.targets)
    )
    fixtures = ast.literal_eval(fixtures_node)
    assert fingerprint(fixtures) == "00d63936aacd2cbdcd5cd535036a31f98b3c132111e35ebef8097434e51d3ff9"
    assert [len(fixture) for fixture in fixtures] == [3, 2, 2]
    assert fixtures[0][0] == fixtures[0][1]
    assert fixtures[0][0]["display_name"] != fixtures[1][0]["display_name"]
    assert fixtures[2][0]["is_deleted"] is True
    assert fixtures[0][-1] == fixtures[1][-1] == fixtures[2][-1]
    client = FakeBundleClient(
        wheel.read_bytes(), display_name="contoso_phase5_chain_probe_dev", library_name=wheel.name,
    )
    result = deploy_phase5_plan(client, plan, tmp_path / "chain-journal.json")
    assert result["items"]["phase5_chain_probe"]["verified"]


def test_bronze_bundle_binds_environment_and_pins_its_synthetic_raw_fixture(tmp_path):
    repository, bundle, wheel_bytes = prepare_bronze_bundle(tmp_path)
    plan = build_phase5_plan(repository, bundle)
    assert plan["scope"] == "PHASE5_BRONZE_PROBE"
    assert plan["runtime_blockers"] == ["QUALITY_REJECT_VAULT_GOLD_AND_SECURITY_ACCEPTANCE"]
    assert plan["environment_library"]["sha256"] == hashlib.sha256(wheel_bytes).hexdigest()
    source = base64.b64decode(plan["items"][0]["definition"]["parts"][0]["payload"]).decode()
    module = ast.parse(source)
    assignments = {
        target.id: node.value
        for node in module.body
        if isinstance(node, ast.Assign)
        for target in node.targets
        if isinstance(target, ast.Name)
    }
    binding = load_json(repository / "environments" / "contoso.dev.json")
    assert json.loads(ast.literal_eval(assignments["environment_json"])) == binding

    notebook_path = ROOT / "fabric" / "items" / "phase5_bronze_probe.Notebook" / "notebook-content.py"
    notebook = ast.parse(notebook_path.read_text(encoding="utf-8"))
    notebook_assignments = {
        target.id: node.value
        for node in notebook.body
        if isinstance(node, ast.Assign)
        for target in node.targets
        if isinstance(target, ast.Name)
    }
    raw_bytes = ast.literal_eval(notebook_assignments["RAW_TEXT"]).encode("utf-8")
    assert len(raw_bytes) == 142
    assert hashlib.sha256(raw_bytes).hexdigest() == ast.literal_eval(
        notebook_assignments["RAW_SHA256"],
    )
    client = FakeBundleClient(
        wheel_bytes,
        notebook_id=BRONZE_NOTEBOOK_ID,
        display_name=plan["items"][0]["displayName"],
    )
    journal = tmp_path / "state" / "bronze-journal.json"
    result = deploy_phase5_plan(client, plan, journal)
    assert result["items"]["phase5_bronze_probe"]["item_id"] == BRONZE_NOTEBOOK_ID
    assert result["items"]["phase5_bronze_probe"]["action"] == "created"
    assert client.library_checks == 1


def test_phase5_bundle_plan_rejects_wheel_hash_drift_and_unsafe_paths(tmp_path):
    repository, bundle, _ = prepare_bundle(tmp_path)
    wheel = repository / "dist" / LIBRARY_NAME
    wheel.write_bytes(wheel.read_bytes() + b"changed")
    with pytest.raises(ContractError, match="SHA-256"):
        build_phase5_plan(repository, bundle)

    _, bundle, _ = prepare_bundle(tmp_path / "unsafe")
    manifest = load_json(bundle)
    manifest["notebooks"][0]["source"] = "../outside.py"
    bundle.write_text(json.dumps(manifest))
    with pytest.raises(ContractError):
        build_phase5_plan(tmp_path / "unsafe", bundle)


def test_phase5_bundle_deploy_is_idempotent_and_verifies_environment_wheel(tmp_path):
    repository, bundle, wheel_bytes = prepare_bundle(tmp_path)
    plan = build_phase5_plan(repository, bundle)
    client = FakeBundleClient(wheel_bytes)
    journal = tmp_path / "state" / "journal.json"
    first = deploy_phase5_plan(client, plan, journal)
    second = deploy_phase5_plan(client, plan, journal)
    assert client.library_checks == 2
    assert client.create_count == 1
    assert second["items"]["phase5_runtime_probe"]["action"] == "verified"
    assert first["items"]["phase5_runtime_probe"]["item_id"] == NOTEBOOK_ID
    assert journal.exists()


def test_published_environment_library_requires_success_and_exact_checksum():
    class EnvironmentClient(ItemClient):
        def __init__(self, library_bytes: bytes, state: str = "Success"):
            super().__init__(lambda *args: pytest.fail("Use deterministic test overrides"))
            self.library_bytes = library_bytes
            self.state = state

        def _get(self, url: str) -> dict:
            return {
                "id": ENVIRONMENT_ID,
                "workspaceId": WORKSPACE,
                "type": "Environment",
                "properties": {"publishDetails": {"state": self.state}},
            }

        def definition(self, workspace_id: str, item_id: str, item_type: str) -> dict:
            return {"parts": [{
                "path": f"Libraries/CustomLibraries/{LIBRARY_NAME}",
                "payloadType": "InlineBase64",
                "payload": base64.b64encode(self.library_bytes).decode(),
            }]}

    client = EnvironmentClient(b"published")
    client.verify_published_library(
        WORKSPACE, ENVIRONMENT_ID, LIBRARY_NAME, hashlib.sha256(b"published").hexdigest(),
    )
    with pytest.raises(DeploymentError, match="checksum"):
        client.verify_published_library(WORKSPACE, ENVIRONMENT_ID, LIBRARY_NAME, "0" * 64)
    with pytest.raises(DeploymentError, match="not published"):
        EnvironmentClient(b"published", state="Running").verify_published_library(
            WORKSPACE, ENVIRONMENT_ID, LIBRARY_NAME, hashlib.sha256(b"published").hexdigest(),
        )
