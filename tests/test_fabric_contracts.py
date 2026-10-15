from __future__ import annotations

import copy
import hashlib
import json
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from fabric_etl.context import BatchContext
from fabric_etl.contracts import ContractError, Environment, fingerprint, hash_key, load_json
from fabric_etl.delivery import (
    Manifest, RegisteredDelivery, next_delivery, validate_ready_delivery_manifest,
)
from fabric_etl.ingestion import (
    RAW_IDENTITY_FIELDS, raw_record_identity, spark_schema, validate_file_inventory,
)


ROOT = Path(__file__).resolve().parents[1]


def binding() -> dict:
    return json.loads((ROOT / "environments" / "contoso.dev.json").read_text())


def manifest() -> dict:
    return {
        "contract_version": "1.0", "project_id": "contoso",
        "source_system_id": "SALES", "delivery_id": "delivery-1",
        "delivery_sequence": 1, "schema_contract_version": "1.0",
        "status": "CLOSED", "objects": [{
            "source_object_id": "SALES.CUSTOMERS",
            "files": [{"path": "customers/part-1.json", "size_bytes": 2,
                       "sha256": fingerprint([])}],
            "complete_snapshot": True, "empty_snapshot": False,
        }],
    }


def parse_manifest(data: dict) -> Manifest:
    return Manifest.from_dict(
        data, project_id="contoso", source_system_id="SALES",
        required_objects={"SALES.CUSTOMERS"}, schema_contract_version="1.0",
    )


def sales_delivery(root: Path) -> dict:
    object_ids = (
        "SALES.CUSTOMERS", "SALES.PRODUCTS", "SALES.ORDERS",
        "SALES.RETURNS", "SALES.EMPLOYEES",
    )
    snapshots = {"SALES.CUSTOMERS", "SALES.PRODUCTS", "SALES.EMPLOYEES"}
    objects = []
    for object_id in object_ids:
        content = f'{{"source_object_id":"{object_id}"}}\n'.encode()
        relative_path = f"{object_id}/part-000.json"
        path = root / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
        objects.append({
            "source_object_id": object_id,
            "files": [{
                "path": relative_path,
                "size_bytes": len(content),
                "sha256": hashlib.sha256(content).hexdigest(),
            }],
            "complete_snapshot": object_id in snapshots,
            "empty_snapshot": False,
        })
    return {
        "contract_version": "1.0", "project_id": "contoso",
        "source_system_id": "SALES", "delivery_id": "sales-001",
        "delivery_sequence": 2, "schema_contract_version": "1.0",
        "status": "CLOSED", "objects": objects,
    }


@pytest.mark.parametrize("content", [
    '{"a":1,"a":2}', '{"nested":{"a":1,"a":2}}', '{"x":NaN}', '{"x":Infinity}',
])
def test_strict_json_rejects_ambiguous_input(tmp_path, content):
    path = tmp_path / "config.json"
    path.write_text(content)
    with pytest.raises(ContractError):
        load_json(path)


def test_original_seed_duplicate_is_detected_without_changing_baseline():
    with pytest.raises(ContractError, match="freshness_sla_hours"):
        load_json(ROOT / "metadata" / "seed" / "meta_source_object.json")


def test_fingerprint_ignores_object_key_order_but_not_array_order():
    assert fingerprint({"b": 2, "a": 1}) == fingerprint({"a": 1, "b": 2})
    assert fingerprint([1, 2]) != fingerprint([2, 1])


def test_namespaces_are_explicit_and_engine_specific():
    env = Environment.from_dict(binding())
    assert env.spark_table("processing", "bronze", "customers") == (
        "`Contoso_lakehouse_ontwikkel`.`lh_contoso_processing`.`bronze`.`customers`"
    )
    assert env.sql_table("current", "customers") == "[current].[customers]"
    with pytest.raises(ContractError, match="Unknown"):
        env.spark_table("raw", "bronze", "customers")
    with pytest.raises(ContractError, match="identifier"):
        env.sql_table("current", "customers]; DROP TABLE x")


@pytest.mark.parametrize("mutation", ["role", "name", "id", "extra", "version"])
def test_environment_rejects_invalid_bindings(mutation):
    data = binding()
    if mutation == "role":
        del data["lakehouses"]["gold"]
    elif mutation == "name":
        data["lakehouses"]["gold"]["name"] = data["lakehouses"]["control"]["name"]
    elif mutation == "id":
        data["workspace_id"] = "not-a-guid"
    elif mutation == "extra":
        data["client_secret"] = "not permitted"
    else:
        data["contract_version"] = "2.0"
    with pytest.raises(ValueError):
        Environment.from_dict(data)


def test_unbound_environment_cannot_be_used_as_provisioned():
    data = binding()
    for entry in data["lakehouses"].values():
        entry["item_id"] = None
    with pytest.raises(ContractError, match="Unprovisioned"):
        Environment.from_dict(data).require_provisioned()


@pytest.mark.parametrize("tenant", [None, "", "not-a-guid"])
def test_environment_rejects_invalid_tenant(tenant):
    data = binding()
    data["tenant_id"] = tenant
    with pytest.raises(ValueError):
        Environment.from_dict(data)


def test_environment_requires_explicit_tenant_and_new_contract_version():
    data = binding()
    del data["tenant_id"]
    with pytest.raises(ContractError):
        Environment.from_dict(data)
    data = binding()
    data["contract_version"] = "1.0"
    with pytest.raises(ContractError, match="version"):
        Environment.from_dict(data)


@pytest.mark.parametrize("left,right", [
    ([None], [""]), ([None], ["^^"]), (["a||b", "c"], ["a", "b||c"]),
    (["a", "b"], ["b", "a"]), (["1"], ["01"]),
])
def test_hash_contract_has_no_legacy_token_or_separator_collision(left, right):
    assert hash_key("SALES", left) != hash_key("SALES", right)


def test_hash_normalization_namespace_and_type_guard():
    assert hash_key("SALES", [" a "]) == hash_key("SALES", ["A"])
    assert hash_key("SALES", ["a"]) != hash_key("CBS", ["a"])
    with pytest.raises(ContractError):
        hash_key("SALES", [1])
    with pytest.raises(ContractError):
        hash_key("SALES", [])


def test_context_roundtrip_preserves_persisted_time_across_attempts():
    context = BatchContext.create(
        "contoso", "SALES", "delivery-1", "release-1",
        load_date=datetime(2026, 10, 10, 8, 0, tzinfo=timezone(timedelta(hours=2))),
    )
    assert context.load_date_utc == "2026-10-10T06:00:00.000000+00:00"
    assert BatchContext.restore(asdict(context)) == context
    other = BatchContext.create(
        "contoso", "SALES", "delivery-1", "release-2",
        load_date=datetime(2026, 10, 10, tzinfo=timezone.utc),
    )
    assert other.batch_id != context.batch_id


def test_context_rejects_naive_time_and_corrupt_identity():
    with pytest.raises(ContractError, match="timezone"):
        BatchContext.create("contoso", "SALES", "d", "r", load_date=datetime(2026, 1, 1))
    context = BatchContext.create(
        "contoso", "SALES", "d", "r", load_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    data = asdict(context)
    data["batch_id"] = "other"
    with pytest.raises(ContractError, match="inconsistent"):
        BatchContext.restore(data)


def test_manifest_verifies_bytes_and_detects_overwrite(tmp_path):
    data = manifest()
    path = tmp_path / "customers" / "part-1.json"
    path.parent.mkdir()
    path.write_bytes(b"[]")
    parsed = parse_manifest(data)
    parsed.verify_local_files(tmp_path)
    path.write_bytes(b"{}")
    with pytest.raises(ContractError, match="content differs"):
        parsed.verify_local_files(tmp_path)


def test_manifest_requires_exact_file_inventory(tmp_path):
    data = manifest()
    path = tmp_path / "customers" / "part-1.json"
    path.parent.mkdir()
    path.write_bytes(b"[]")
    parsed = parse_manifest(data)
    (tmp_path / "unlisted.json").write_text("{}")
    with pytest.raises(ContractError, match="unexpected"):
        parsed.verify_local_files(tmp_path)
    (tmp_path / "unlisted.json").unlink()
    path.unlink()
    with pytest.raises(ContractError, match="missing"):
        parsed.verify_local_files(tmp_path)


def test_spark_source_schema_uses_declared_types_and_nullability():
    schema = spark_schema([
        {"name": "business_key", "data_type": "STRING", "nullable": False},
        {"name": "amount", "data_type": "DECIMAL(18,4)", "nullable": True},
    ])
    assert [(field.name, field.dataType.simpleString(), field.nullable) for field in schema.fields] == [
        ("business_key", "string", False),
        ("amount", "decimal(18,4)", True),
    ]


def test_remote_file_inventory_must_match_manifest_exactly():
    data = manifest()
    parsed = parse_manifest(data)
    validate_file_inventory(parsed, ["customers/part-1.json", "_manifest.json"])
    with pytest.raises(ContractError, match="unexpected"):
        validate_file_inventory(parsed, ["customers/part-1.json", "extra.json"])
    with pytest.raises(ContractError, match="missing"):
        validate_file_inventory(parsed, [])


def test_raw_record_ids_preserve_duplicate_multiplicity_and_file_identity():
    common = {
        "project_id": "contoso",
        "source_system_id": "SALES",
        "delivery_id": "SALES|2026-10-11",
        "source_object_id": "SALES.CUSTOMERS",
        "source_file": "customers/part-000.json",
        "file_sha256": "a" * 64,
        "payload_sha256": "b" * 64,
    }
    first = raw_record_identity(**common, duplicate_ordinal=1)
    second = raw_record_identity(**common, duplicate_ordinal=2)
    assert first != second
    assert first == raw_record_identity(**common, duplicate_ordinal=1)
    changed_file = {**common, "file_sha256": "c" * 64}
    assert first != raw_record_identity(**changed_file, duplicate_ordinal=1)


def test_raw_record_id_matches_cross_engine_json_hash_vector():
    assert RAW_IDENTITY_FIELDS == tuple(sorted(RAW_IDENTITY_FIELDS))
    assert raw_record_identity(
        project_id="contoso",
        source_system_id="SALES",
        delivery_id="SALES|2026-09-01",
        source_object_id="SALES.CUSTOMERS",
        source_file="customers/part-000.json",
        file_sha256="a" * 64,
        payload_sha256="b" * 64,
        duplicate_ordinal=2,
    ) == "90d2b85ced455a8fee9cb1a2644b73a8e5c20ccd4f95e55786f0871937038810"


def test_sales_delivery_gate_checks_closed_scope_chronology_and_file_bytes(tmp_path):
    data = sales_delivery(tmp_path)
    required_objects = {
        "SALES.CUSTOMERS", "SALES.PRODUCTS", "SALES.ORDERS",
        "SALES.RETURNS", "SALES.EMPLOYEES",
    }
    validated = validate_ready_delivery_manifest(
        data,
        root=tmp_path,
        project_id="contoso",
        source_system_id="SALES",
        required_objects=required_objects,
        schema_contract_version="1.0",
        registered_deliveries=[
            RegisteredDelivery("sales-001", 2, "READY"),
            RegisteredDelivery("sales-old", 1, "SUCCEEDED"),
        ],
    )
    assert validated.delivery_id == "sales-001"
    assert {entry.source_object_id for entry in validated.objects} == required_objects

    first_file = tmp_path / data["objects"][0]["files"][0]["path"]
    first_file.write_bytes(b'{"overwritten":true}\n')
    with pytest.raises(ContractError, match="content differs"):
        validate_ready_delivery_manifest(
            data,
            root=tmp_path,
            project_id="contoso",
            source_system_id="SALES",
            required_objects=required_objects,
            schema_contract_version="1.0",
            registered_deliveries=[RegisteredDelivery("sales-001", 2, "READY")],
        )


def test_delivery_gate_blocks_out_of_order_or_unready_manifest(tmp_path):
    data = sales_delivery(tmp_path)
    arguments = {
        "root": tmp_path,
        "project_id": "contoso",
        "source_system_id": "SALES",
        "required_objects": {
            "SALES.CUSTOMERS", "SALES.PRODUCTS", "SALES.ORDERS",
            "SALES.RETURNS", "SALES.EMPLOYEES",
        },
        "schema_contract_version": "1.0",
    }
    with pytest.raises(ContractError, match="chronologically processable"):
        validate_ready_delivery_manifest(
            data,
            **arguments,
            registered_deliveries=[
                RegisteredDelivery("sales-older", 1, "PROCESSING"),
                RegisteredDelivery("sales-001", 2, "READY"),
            ],
        )
    with pytest.raises(ContractError, match="chronologically next"):
        validate_ready_delivery_manifest(
            data,
            **arguments,
            registered_deliveries=[
                RegisteredDelivery("sales-001", 2, "READY"),
                RegisteredDelivery("sales-next", 3, "READY"),
                RegisteredDelivery("sales-earlier", 1, "READY"),
            ],
        )


@pytest.mark.parametrize("path", [
    "../escape", "/absolute", "C:/drive", "a\\b", "a//b", "./a",
    "a/../b", "a:b", "a/.", "a/",
])
def test_manifest_rejects_unsafe_paths(path):
    data = manifest()
    data["objects"][0]["files"][0]["path"] = path
    with pytest.raises(ContractError, match="path"):
        parse_manifest(data)


def test_manifest_cannot_claim_its_own_root_manifest_as_a_data_file():
    data = manifest()
    data["objects"][0]["files"][0]["path"] = "_manifest.json"
    with pytest.raises(ContractError, match="path"):
        parse_manifest(data)


@pytest.mark.parametrize("field,value", [
    ("status", "OPEN"), ("delivery_sequence", True), ("delivery_sequence", 0),
    ("schema_contract_version", "2.0"), ("project_id", "other"),
])
def test_manifest_rejects_bad_header(field, value):
    data = manifest()
    data[field] = value
    with pytest.raises(ContractError):
        parse_manifest(data)


def test_only_explicit_complete_empty_snapshot_may_omit_files():
    data = manifest()
    obj = data["objects"][0]
    obj["files"] = []
    with pytest.raises(ContractError):
        parse_manifest(data)
    obj["empty_snapshot"] = True
    assert parse_manifest(data).objects[0].empty_snapshot
    obj["complete_snapshot"] = False
    with pytest.raises(ContractError):
        parse_manifest(data)


def test_manifest_rejects_duplicate_objects_and_wrong_scope():
    data = manifest()
    data["objects"].append(copy.deepcopy(data["objects"][0]))
    with pytest.raises(ContractError, match="Duplicate"):
        parse_manifest(data)
    data = manifest()
    data["objects"][0]["source_object_id"] = "SALES.PRODUCTS"
    with pytest.raises(ContractError, match="scope"):
        parse_manifest(data)


@pytest.mark.parametrize("status", [
    "REGISTERED", "WAITING", "PROCESSING", "INVALID", "FAILED", "QUARANTINED",
])
def test_oldest_unfinished_delivery_blocks_newer_ready_delivery(status):
    assert next_delivery([
        RegisteredDelivery("new", 2, "READY"),
        RegisteredDelivery("old", 1, status),
    ]) is None


def test_chronology_allows_only_completed_or_explicitly_superseded_predecessors():
    ready = RegisteredDelivery("new", 3, "READY")
    assert next_delivery([
        ready, RegisteredDelivery("old", 1, "SUCCEEDED"),
        RegisteredDelivery("replaced", 2, "SUPERSEDED"),
    ]) == ready
    assert next_delivery([]) is None
    with pytest.raises(ContractError):
        next_delivery([ready, RegisteredDelivery("duplicate", 3, "READY")])
