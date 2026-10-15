from __future__ import annotations

import copy
import shutil
from pathlib import Path

import pytest

from fabric_etl.contoso import _declared_tables, _quality_mapping_expression, _source_objects, convert
from fabric_etl.contracts import ContractError, fingerprint, load_json
from fabric_etl.project import ProjectRelease, data_type, safe_file
from fabric_etl.__main__ import main


ROOT = Path(__file__).resolve().parents[1]
PROJECT = ROOT / "projects" / "contoso"


def document():
    return copy.deepcopy(load_json(PROJECT / "release.json"))


def seal(value):
    value["release_id"] = fingerprint({k: v for k, v in value.items() if k != "release_id"})
    return value


def validate(value):
    return ProjectRelease.validate(seal(value), PROJECT)


def test_converted_project_has_exact_scope_and_excludes_sharepoint():
    project = ProjectRelease.load(PROJECT / "release.json")
    assert len(project.document["source_systems"]) == 5
    assert {r["id"] for r in project.document["source_objects"]} == {
        "SALES.CUSTOMERS", "SALES.PRODUCTS", "SALES.ORDERS", "SALES.RETURNS",
        "SALES.EMPLOYEES", "CBS.JEUGDZORG_WIJK_2025", "ECB.EXCHANGE_RATE",
        "NAGER.HOLIDAYS_NL", "FABRIC_SALES.ORDER_LINES",
    }
    assert len(project.document["products"]) == 5
    assert len(project.document["gold_entities"]) == 19
    assert len(project.document["mappings"]) == 153
    assert len(project.document["dependencies"]) == 46
    assert sum(r["entity_id"].startswith("business_entities:") for r in project.document["mappings"]) == 9
    for source in ("ECB", "NAGER", "FABRIC_SALES"):
        reference = next(r for r in project.document["reference_entities"]
                         if r["source_object_id"].startswith(source + "."))
        gold = next(r for r in project.document["gold_entities"]
                    if r["source_system_id"] == source and r["layer"] == "HISTORICAL")
        assert any(r["entity"] == "gold_entities:" + gold["id"]
                   and r["depends_on"] == "reference_entities:" + reference["id"]
                   for r in project.document["dependencies"])
    assert not any("SHAREPOINT" in r["id"] for name in (
        "source_objects", "source_systems", "gold_entities", "products",
    ) for r in project.document[name])
    assert len(project.topological_order) == len(set(project.topological_order))
    assert project.runtime_blockers


def test_candidate_cannot_be_misrepresented_as_runtime_ready():
    with pytest.raises(ContractError, match="Runtime release blocked"):
        ProjectRelease.load(PROJECT / "release.json", require_runtime=True)


def test_gold_has_full_typed_output_declarations_not_just_grain():
    project = ProjectRelease.load(PROJECT / "release.json").document
    schemas = {schema["id"]: schema for schema in project["schemas"]}
    for entity in project["gold_entities"]:
        columns = schemas[entity["schema_id"]]["columns"]
        assert len(columns) > len(entity["keys"])
        assert any(column["name"] == "_batch_id" for column in columns)
        assert schemas[entity["schema_id"]]["status"] == "REQUIRES_COMPILE"
    date = schemas["SCHEMA_GC_DIM_DATE"]
    assert next(c for c in date["columns"] if c["name"] == "date_key")["data_type"] == "INT"


def test_legacy_column_extraction_is_strict_and_handles_decimal_and_comments(tmp_path):
    ddl = tmp_path / "ddl.sql"
    ddl.write_text("CREATE TABLE IF NOT EXISTS sample (key STRING NOT NULL, "
                   "amount DECIMAL(18,4) COMMENT 'amount, in EUR', "
                   "CONSTRAINT pk PRIMARY KEY (key) RELY) USING DELTA;")
    columns = _declared_tables(ddl)["sample"]
    assert [col["data_type"] for col in columns] == ["STRING", "DECIMAL(18,4)"]
    assert not columns[0]["nullable"] and columns[1]["nullable"]
    ddl.write_text("CREATE TABLE IF NOT EXISTS sample (key STRUCT<a:STRING>) USING DELTA;")
    with pytest.raises(ContractError, match="Unsupported legacy"):
        _declared_tables(ddl)


def test_conversion_is_repeatable_and_matches_checked_project(tmp_path):
    output = tmp_path / "project"
    first = convert(ROOT / "metadata" / "seed", output)
    second = convert(ROOT / "metadata" / "seed", output)
    assert first == second == document()
    generated = output / "sql" / "GH_DIM_CUSTOMER.sql"
    generated.write_text("SELECT changed\n")
    with pytest.raises(ContractError, match="overwrite"):
        convert(ROOT / "metadata" / "seed", output)
    assert generated.read_text() == "SELECT changed\n"


def test_verified_fabric_sql_view_mapping_does_not_reference_absent_total_due_column():
    seed = next(m for m in load_json(ROOT / "metadata" / "seed" / "meta_mapping.json")
                if m["mapping_id"] == "M-QA-FAB-05")
    expected = "cast(subtotal_amount + tax_amount + freight_amount as decimal(18,4))"
    assert _quality_mapping_expression(seed) == expected
    current = next(m for m in document()["mappings"] if m["id"] == "M-QA-FAB-05")
    assert current["expression"] == expected
    for field in ("source_object_id", "target_column", "target_data_type", "source_expression"):
        changed = dict(seed, **{field: "unexpected"})
        with pytest.raises(ContractError, match="Unexpected legacy"):
            _quality_mapping_expression(changed)


def test_other_quality_mappings_are_not_rewritten():
    for seed in load_json(ROOT / "metadata" / "seed" / "meta_mapping.json"):
        if seed["mapping_id"] != "M-QA-FAB-05":
            assert _quality_mapping_expression(seed) == seed["source_expression"]


def test_only_reviewed_duplicate_seed_is_allowed(tmp_path):
    assert _source_objects(ROOT / "metadata" / "seed" / "meta_source_object.json")[0]["freshness_sla_hours"] == 24
    path = tmp_path / "seed.json"
    path.write_text('[{"source_object_id":"SALES.CUSTOMERS","freshness_sla_hours":26,"freshness_sla_hours":25}]')
    with pytest.raises(ContractError, match="Unexpected legacy"):
        _source_objects(path)
    path.write_text('[]')
    with pytest.raises(ContractError, match="exactly"):
        _source_objects(path)


@pytest.mark.parametrize("field", ["contract_version", "framework_version", "schema_contract_version", "hash_contract_version"])
def test_incompatible_versions_fail(field):
    data = document()
    data[field] = "unsupported"
    with pytest.raises(ContractError, match="version"):
        validate(data)


@pytest.mark.parametrize("collection", [
    "source_systems", "source_objects", "connectors", "schemas", "mappings",
    "filters", "quality_rules", "dependencies", "vault_entities",
    "reference_entities", "business_entities", "gold_entities", "products",
    "policies", "expressions",
])
def test_every_collection_rejects_unknown_fields_and_duplicate_ids(collection):
    data = document()
    data[collection][0]["client_secret"] = "forbidden"
    with pytest.raises(ContractError, match="requires exactly"):
        validate(data)
    data = document()
    data[collection].append(copy.deepcopy(data[collection][0]))
    with pytest.raises(ContractError, match="Duplicate"):
        validate(data)


@pytest.mark.parametrize("collection", [
    "source_systems", "source_objects", "connectors", "schemas", "products",
    "gold_entities", "policies",
])
def test_required_collections_may_not_be_empty(collection):
    data = document()
    data[collection] = []
    with pytest.raises(ContractError):
        validate(data)


@pytest.mark.parametrize("field", ["connector_id", "source_system_id", "schema_id"])
def test_source_foreign_keys_are_checked(field):
    data = document()
    data["source_objects"][0][field] = "missing"
    with pytest.raises(ContractError, match="Dangling"):
        validate(data)


def test_dag_cycle_and_dangling_parent_are_rejected():
    data = document()
    first, second = "source_objects:SALES.CUSTOMERS", "source_objects:SALES.PRODUCTS"
    data["dependencies"].extend([
        {"id": "cycle_a", "entity": first, "depends_on": second},
        {"id": "cycle_b", "entity": second, "depends_on": first},
    ])
    with pytest.raises(ContractError, match="Cyclic"):
        validate(data)
    data = document()
    data["vault_entities"][0]["parents"] = ["vault_entities:missing"]
    with pytest.raises(ContractError, match="Unknown parent"):
        validate(data)


def test_duplicate_targets_columns_and_ordinals_are_rejected():
    data = document()
    data["source_objects"][1]["targets"] = data["source_objects"][0]["targets"]
    with pytest.raises(ContractError, match="Duplicate physical target"):
        validate(data)
    data = document()
    data["schemas"][0]["columns"][1]["ordinal"] = 1
    with pytest.raises(ContractError, match="Duplicate schema"):
        validate(data)
    data = document()
    data["mappings"][1]["target_column"] = data["mappings"][0]["target_column"]
    with pytest.raises(ContractError, match="Duplicate mapping"):
        validate(data)


def test_quality_mapping_and_declared_keys_are_required():
    data = document()
    data["mappings"] = [r for r in data["mappings"] if r["source_object_id"] != "SALES.CUSTOMERS"]
    with pytest.raises(ContractError, match="Missing Quality"):
        validate(data)
    data = document()
    data["source_objects"][0]["keys"] = ["unknown_key"]
    with pytest.raises(ContractError, match="Key missing"):
        validate(data)


@pytest.mark.parametrize("collection,field,value", [
    ("mappings", "source_object_id", None),
    ("mappings", "target_column", "missing"),
    ("vault_entities", "hash_columns", ["missing"]),
    ("gold_entities", "source_system_id", None),
])
def test_mapping_and_hash_schema_guards(collection, field, value):
    data = document()
    data[collection][0][field] = value
    with pytest.raises(ContractError):
        validate(data)


@pytest.mark.parametrize("value", ["DECIMAL(39,0)", "DECIMAL(2,3)", "ARRAY<STRING>", "TIMESTAMP_NTZ", "DECIMAL(0,0)", None])
def test_unsupported_datatypes_fail(value):
    with pytest.raises(ContractError):
        data_type(value)


@pytest.mark.parametrize("value", ["STRING", "DECIMAL(38,18)", "BOOLEAN", "TIMESTAMP", "LONG"])
def test_supported_datatypes(value):
    assert data_type(value) == value


def test_nager_complex_payload_has_explicit_string_projection():
    data = document()
    mapping = next(r for r in data["mappings"] if r["source_object_id"] == "NAGER.HOLIDAYS_NL"
                   and r["target_column"] == "holiday_types")
    assert mapping["data_type"] == "STRING"
    assert mapping["expression"].startswith("to_json(")


@pytest.mark.parametrize("path", ["../secret", "C:/secret", "/secret", "a\\b", "a//b", "./release.json"])
def test_project_manifest_disallows_path_escape(path):
    with pytest.raises(ContractError):
        safe_file(PROJECT, path)


def test_project_and_external_sql_fingerprints_are_verified(tmp_path):
    data = document()
    data["source_objects"][0]["freshness_hours"] = 25
    with pytest.raises(ContractError, match="release fingerprint"):
        ProjectRelease.validate(data, PROJECT)
    root = tmp_path / "project"
    shutil.copytree(PROJECT, root)
    (root / "sql" / "GH_FCT_SALES.sql").write_text("SELECT changed\n")
    with pytest.raises(ContractError, match="file fingerprint"):
        ProjectRelease.load(root / "release.json")


def test_connector_unknown_types_and_credentials_are_rejected():
    data = document()
    data["connectors"][0]["type"] = "unknown"
    data["connectors"][0] = dict(reversed(list(data["connectors"][0].items())))
    with pytest.raises(ContractError, match="Unsupported"):
        validate(data)
    data = document()
    connector = next(r for r in data["connectors"] if r["type"] == "HTTP_JSON")
    connector["endpoint"] = "https://user:password@example.com/data"
    with pytest.raises(ContractError, match="credential"):
        validate(data)


def test_jdbc_query_reference_and_product_membership_required():
    data = document()
    connector = next(r for r in data["connectors"] if r["type"] == "JDBC")
    connector["parameters"]["source_query_ref"] = "missing"
    with pytest.raises(ContractError, match="JDBC query"):
        validate(data)
    data = document()
    data["products"][0]["entities"] = []
    with pytest.raises(ContractError):
        validate(data)


@pytest.mark.parametrize("value", [True, -1, 101, float("nan")])
def test_quality_threshold_is_bounded_number(value):
    data = document()
    data["quality_rules"][0]["threshold_pct"] = value
    with pytest.raises((ContractError, ValueError)):
        validate(data)


def test_warning_rules_do_not_remove_records():
    data = document()
    data["quality_rules"][0].update(severity="WARNING", action="REJECT")
    with pytest.raises(ContractError, match="WARNING"):
        validate(data)


def test_runtime_cli_exits_nonzero_instead_of_candidate_success(capsys):
    assert main(["validate-project", str(PROJECT / "release.json"), "--require-runtime"]) == 1
    captured = capsys.readouterr()
    assert "Runtime release blocked" in captured.err
    assert not captured.out
