import hashlib
import json
from pathlib import Path

import pytest

from fabric_etl.contracts import canonical_json
from fabric_etl.project import ProjectRelease


ROOT = Path(__file__).resolve().parents[1]
CONTRACTS = ROOT / "projects" / "contoso" / "reference-tests"


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


@pytest.mark.parametrize("name,file_field", [
    ("c0-design-v0.1", "contract_file"),
    ("c0-design-v0.1-seed-evidence-addendum", "addendum_file"),
    ("c0-fabric-schema-preparation", "preparation_file"),
    ("c0-cross-platform-matrix", "matrix_file"),
    ("c0-mapped-relationship-addendum", "addendum_file"),
])
def test_reference_document_matches_external_digest(name, file_field):
    pin = read_json(CONTRACTS / (name + ".sha256.json"))
    payload = canonical_json(read_json(CONTRACTS / pin[file_field]))
    assert hashlib.sha256(payload).hexdigest() == pin["full_sha256"]
    assert len(payload) == pin["canonical_bytes"]


def test_input_files_and_cutoffs_match_existing_quality_evidence():
    contract = read_json(CONTRACTS / "c0-design-v0.1.json")
    receipt = read_json(ROOT / "deploy" / "evidence" / "phase6-sales-quality-v1-receipt.json")
    files = []
    for delivery in receipt["job"]["exit_result"]["deliveries"]:
        number = delivery["delivery_number"]
        files.extend({"delivery": f"D{number}", **item} for item in delivery["ingest"]["files"])
        mapping = contract["time"]["platform_mapping"]["fabric"][number - 1]
        assert mapping["batch_id"] == delivery["ingest"]["batch_id"]
        assert mapping["fixture_fingerprint"] == delivery["fixture_fingerprint"]
        assert mapping["cutoff"].replace("Z", "+00:00") == delivery["ingest"]["load_date_utc"]
    assert contract["input"]["files"] == files
    assert len(files) == 15


def test_fabric_product_matrix_accounts_for_all_schema_columns():
    matrix = read_json(CONTRACTS / "c0-fabric-schema-preparation.json")
    source = ROOT / matrix["fabric_schema_source"]["path"]
    assert hashlib.sha256(source.read_bytes()).hexdigest() == matrix["fabric_schema_source"]["sha256"]
    project = ProjectRelease.load(source)
    assert project.release_id == matrix["fabric_schema_source"]["project_release_id"]
    schemas = {schema["id"]: schema for schema in project.document["schemas"]}
    assert {item["id"] for item in matrix["products"]} == {
        item["id"] for item in project.document["gold_entities"] if item["product_id"] == "SALES_MART"
    }
    temporal = set(matrix["comparison_partition"]["temporal_state"])
    for product in matrix["products"]:
        columns = schemas[product["schema_id"]]["columns"]
        business = [
            column["name"] for column in columns
            if not column["name"].startswith("_") and not column["name"].endswith("_hk")
            and column["name"] not in temporal and column["name"] != "record_source"
        ]
        assert business == product["ordered_business_columns"]
        assert len(business) == len(set(business))
        if "FCT_SALES" in product["id"]:
            assert next(column for column in columns if column["name"] == "discount_rate")["data_type"] == "DECIMAL(9,6)"


def test_designation_does_not_authorize_execution_or_choose_dates():
    contract = read_json(CONTRACTS / "c0-design-v0.1.json")
    decision = read_json(CONTRACTS / "c0-reference-designation.json")
    assert decision["base_contract_full_sha256"] == hashlib.sha256(canonical_json(contract)).hexdigest()
    assert decision["designated_workspace_id"] == "7405617094653375"
    assert decision["execution_status"] == contract["readiness"]["execution_status"] == "HOLD"
    for action in ("provisioning", "compute", "jobs", "fixture_transfer", "upload", "auth_changes", "business_code_corrections"):
        assert decision[action] is False
        assert contract["authorization"][action] is False
    assert contract["time"]["business_reference_date"] is None
    assert all(item["date_folder"] is None for item in contract["time"]["platform_mapping"]["databricks"])
    assert [item["id"] for item in contract["readiness"]["gates"]] == [f"G{number}" for number in range(1, 10)]


def test_local_oracle_pins_and_evidence_addendum_remain_consistent():
    contract = read_json(CONTRACTS / "c0-design-v0.1.json")
    for pin in contract["evidence"]["local_pins"]:
        assert hashlib.sha256((ROOT / pin["path"]).read_bytes()).hexdigest() == pin["sha256"]
    addendum = read_json(CONTRACTS / "c0-design-v0.1-seed-evidence-addendum.json")
    assert addendum["base_contract_full_sha256"] == hashlib.sha256(canonical_json(contract)).hexdigest()
    assert len(addendum["artifacts"]) == 6
    assert len({item["logical_path"] for item in addendum["artifacts"]}) == 6
    assert addendum["evidence_defaults"]["active_metadata_row_hashes"] is False
    assert addendum["contract_independently_validated_by_databricks_session"] is False


def test_cross_platform_matrix_matches_declared_fabric_columns_and_types():
    matrix = read_json(CONTRACTS / "c0-cross-platform-matrix.json")
    preparation = read_json(CONTRACTS / "c0-fabric-schema-preparation.json")
    assert matrix["fabric_preparation_full_sha256"] == hashlib.sha256(canonical_json(preparation)).hexdigest()
    project = ProjectRelease.load(ROOT / preparation["fabric_schema_source"]["path"])
    schemas = {schema["id"]: schema for schema in project.document["schemas"]}
    product_schemas = {item["id"]: item["schema_id"] for item in preparation["products"]}
    types = matrix["column_notation"]["types"]
    assert {item["id"] for item in matrix["products"]} == set(product_schemas)
    derived = {"product_key", "customer_key", "employee_key"}
    for product in matrix["products"]:
        business = matrix["business_bases"][product["business_base"]] + product["business_append"]
        temporal = matrix["temporal_bases"][product["temporal_base"]]
        technical = matrix["technical_bases"][product["technical_base"]] + matrix["audit_bases"][product["audit_base"]]
        entries = business + temporal + technical
        names = [entry.split(":")[0] for entry in entries]
        assert len(names) == len(set(names))
        declared = {column["name"]: column for column in schemas[product_schemas[product["id"]]]["columns"]}
        fact_derived = derived if "_FCT_" in product["id"] else set()
        assert set(names) == set(declared) | fact_derived
        for entry in entries:
            name, kind, nullability = entry.split(":")
            if name in fact_derived:
                continue
            assert declared[name]["data_type"] == types[kind], (product["id"], name)
            assert declared[name]["nullable"] == (nullability == "?"), (product["id"], name)
        assert set(product["natural_grain"]).issubset(set(names))
    assert matrix["fact_key_projection"]["maps_from_filtered_gc_dimensions"] is False
    assert matrix["fact_key_projection"]["maps_verified"] is False
    assert matrix["execution_status"] == "HOLD"
    assert matrix["output_equivalence"] == "NOT_TESTED"


def test_relationship_addendum_preserves_physical_columns_and_explicit_aliases():
    addendum = read_json(CONTRACTS / "c0-mapped-relationship-addendum.json")
    for field, filename in [
        ("base_contract_full_sha256", "c0-design-v0.1.json"),
        ("fabric_preparation_full_sha256", "c0-fabric-schema-preparation.json"),
        ("cross_platform_matrix_full_sha256", "c0-cross-platform-matrix.json"),
    ]:
        assert hashlib.sha256(canonical_json(read_json(CONTRACTS / filename))).hexdigest() == addendum[field]
    relationships = addendum["mapped_relationship_business_columns"]
    assert [item["name"] for item in relationships] == [
        "mapped_product_key", "mapped_customer_key", "mapped_employee_key"
    ]
    assert [item["nullable_declared"] for item in relationships] == [False, True, True]
    preparation = read_json(CONTRACTS / "c0-fabric-schema-preparation.json")
    physical = {product["id"]: product["ordered_business_columns"] for product in preparation["products"]}
    for product_id, spec in addendum["fact_products"].items():
        assert "mapped_product_key" in spec["natural_grain"]
        assert "mapped_customer_key" not in spec["natural_grain"]
        assert "mapped_employee_key" not in spec["natural_grain"]
        assert not set(item["name"] for item in relationships).intersection(physical[product_id])
    assert addendum["mapping_rules"]["non_null_fk_without_unique_match"] == "MAPPING_ERROR"
    assert addendum["full_comparator_pin"] == "PENDING"
    assert addendum["execution_status"] == "HOLD"
