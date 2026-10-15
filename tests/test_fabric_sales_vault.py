import copy
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from fabric_etl.contracts import ContractError
from fabric_etl.project import ProjectRelease
from fabric_etl_phase6.vault import (
    business_expression, read_history, read_vault_commit, vault_order, vault_spec, write_vault,
)


ROOT = Path(__file__).resolve().parents[1]


def project():
    return ProjectRelease.load(ROOT / "projects" / "contoso" / "release.json")


def test_every_sales_raw_vault_entity_compiles_against_the_actual_quality_schema():
    release = project()
    entities = [
        entity for entity in release.document["vault_entities"]
        if all(source.startswith("SALES.") for source in entity["source_objects"])
    ]
    assert len(entities) == 16
    for entity in entities:
        spec = vault_spec(release, entity["id"])
        assert spec["source_id"].startswith("SALES.")
        assert spec["key_column"].startswith("hk_")
        assert spec["identity_columns"]
        if entity["type"] in {"SATELLITE", "LINK_SATELLITE"}:
            assert spec["hash_columns"] == entity["hash_columns"]


def test_business_satellites_compile_but_pit_cannot_be_silently_skipped():
    release = project()
    assert vault_spec(release, "SAT_CUSTOMER_BV", business=True)["key_column"] == "hk_customer"
    assert vault_spec(release, "SAT_ORDER_LINE_BV", business=True)["key_column"] == "hk_order_product"
    with pytest.raises(ContractError, match="PIT requires"):
        vault_spec(release, "PIT_CUSTOMER", business=True)


def test_unmodelled_or_non_sales_entities_do_not_fall_back_to_a_generic_loader():
    with pytest.raises(ContractError, match="Unknown"):
        vault_spec(project(), "MISSING")
    with pytest.raises(ContractError, match="another source"):
        vault_spec(project(), "HUB_CBS_JEUGDZORG")


def test_satellite_hashdiff_order_is_an_explicit_contract():
    release = project()
    entity = next(entity for entity in release.document["vault_entities"] if entity["id"] == "SAT_CUSTOMER")
    entity["hash_columns"].reverse()
    with pytest.raises(ContractError, match="hashdiff order"):
        vault_spec(release, "SAT_CUSTOMER")


def test_vault_order_resolves_every_sales_identity_parent_before_its_consumer():
    release = project()
    ordered = vault_order(release)
    assert len(ordered) == 16
    for entity_id in ordered:
        for parent in vault_spec(release, entity_id)["entity"]["parents"]:
            assert ordered.index(parent.split(":", 1)[1]) < ordered.index(entity_id)
    assert set(vault_order(release, business=True)) == {"SAT_CUSTOMER_BV", "SAT_ORDER_LINE_BV"}


def test_vault_order_cannot_silently_skip_missing_sales_parents():
    release = project()
    release.document["vault_entities"] = [
        entity for entity in release.document["vault_entities"] if entity["id"] != "HUB_CUSTOMER"
    ]
    with pytest.raises(ContractError, match="omits a declared identity parent"):
        vault_order(release)


@pytest.mark.parametrize("expression,expected", [
    ("months_between(current_date(), customer_since)", "months_between(cast(load_date AS DATE), customer_since)"),
    ("CURRENT_DATE ( )", "cast(load_date AS DATE)"),
    ("'current_date()' || current_date()", "'current_date()' || cast(load_date AS DATE)"),
    ("'it''s current_date()' || current_date()", "'it''s current_date()' || cast(load_date AS DATE)"),
    ('"current_date()" || current_date()', '"current_date()" || cast(load_date AS DATE)'),
    ("`current_date()` || current_date()", "`current_date()` || cast(load_date AS DATE)"),
    ("my_current_date() || namespace.current_date()", "my_current_date() || namespace.current_date()"),
])
def test_business_date_binding_does_not_rewrite_literals_identifiers_or_other_functions(expression, expected):
    assert business_expression(expression) == expected


class Environment:
    def spark_table(self, role, schema, table):
        return f"{role}.{schema}.{table}"


class Control:
    project = "contoso"
    environment = Environment()
    spark = object()

    def table(self, name):
        return name


@pytest.mark.parametrize("proof_only,table", [(False, "phase6_hub_customer"), (True, "hub_customer")])
def test_vault_writer_requires_explicit_isolated_proof_scope(proof_only, table):
    with pytest.raises(ContractError, match="isolated phase-six"):
        write_vault(
            Control(), None, None, None, project=project(), entity_id="HUB_CUSTOMER",
            input_commit_ids=[], history_commit_ids=[], table=table, now=datetime.now(timezone.utc),
            proof_only=proof_only,
        )


def test_history_read_rejects_duplicate_commit_ids(monkeypatch):
    monkeypatch.setattr("fabric_etl_phase6.vault.rows",
                        lambda *args: pytest.fail("Duplicate inventory must fail before a control read"))
    with pytest.raises(ContractError, match="duplicated"):
        read_history(Control(), project=project(), entity_id="HUB_CUSTOMER", table="phase6_hub_customer",
                     commit_ids=["prior", "prior"], empty=object(), before_batch_id="current")


def batch(release):
    return {"project_id": "contoso", "source_system_id": "PHASE4_PROOF", "release_id": release.release_id}


@pytest.mark.parametrize("mutation", ["missing", "duplicate", "project_id", "source_system_id", "release_id"])
def test_history_read_requires_a_real_bound_proof_batch(monkeypatch, mutation):
    release = project()
    stored = [batch(release)]
    if mutation == "missing":
        stored = []
    elif mutation == "duplicate":
        stored.append(copy.deepcopy(stored[0]))
    else:
        stored[0][mutation] = "foreign"
    monkeypatch.setattr("fabric_etl_phase6.vault.rows", lambda *args: stored)
    with pytest.raises(ContractError, match="persisted proof batch"):
        read_history(Control(), project=release, entity_id="HUB_CUSTOMER", table="phase6_hub_customer",
                     commit_ids=[], empty=object(), before_batch_id="current")


@pytest.mark.parametrize("inventory,requested", [
    ([], ["prior"]), ([{"commit_id": "prior"}], []),
    ([{"commit_id": "prior"}, {"commit_id": "another"}], ["prior"]),
    ([{"commit_id": "foreign"}], ["prior"]),
])
def test_history_read_cannot_omit_or_invent_prior_commits(monkeypatch, inventory, requested):
    release = project()
    monkeypatch.setattr("fabric_etl_phase6.vault.rows",
                        lambda spark, query: inventory if "JOIN" in query else [batch(release)])
    with pytest.raises(ContractError, match="omits or adds"):
        read_history(Control(), project=release, entity_id="HUB_CUSTOMER", table="phase6_hub_customer",
                     commit_ids=requested, empty=object(), before_batch_id="current")


@pytest.mark.parametrize("mutation", ["missing", "duplicate", "status", "entity_id", "layer", "target"])
def test_history_read_rechecks_each_authoritative_manifest_before_reading_rows(monkeypatch, mutation):
    release = project()
    row = {
        "status": "COMMITTED", "entity_id": "HUB_CUSTOMER", "layer": "RAW_VAULT",
        "outputs_json": json.dumps({"target_table": "processing.raw_vault.phase6_hub_customer"}),
    }
    stored = [row]
    if mutation == "missing":
        stored = []
    elif mutation == "duplicate":
        stored.append(copy.deepcopy(row))
    elif mutation == "target":
        row["outputs_json"] = json.dumps({"target_table": "foreign.table"})
    else:
        row[mutation] = "foreign"

    def read(spark, query):
        if "JOIN" in query:
            return [{"commit_id": "prior"}]
        return [batch(release)] if "batch_context" in query else stored

    monkeypatch.setattr("fabric_etl_phase6.vault.rows", read)
    with pytest.raises(ContractError, match="target differs" if mutation == "target" else "authoritative"):
        read_history(Control(), project=release, entity_id="HUB_CUSTOMER", table="phase6_hub_customer",
                     commit_ids=["prior"], empty=object(), before_batch_id="current")


@pytest.mark.parametrize("mutation", ["missing", "duplicate", "status", "entity_id", "layer", "batch_id", "target"])
def test_current_vault_commit_read_rejects_foreign_or_non_authoritative_outputs(monkeypatch, mutation):
    release = project()
    row = {
        "status": "COMMITTED", "entity_id": "HUB_CUSTOMER", "layer": "RAW_VAULT", "batch_id": "current",
        "outputs_json": json.dumps({"target_table": "processing.raw_vault.phase6_hub_customer"}),
    }
    stored = [row]
    if mutation == "missing":
        stored = []
    elif mutation == "duplicate":
        stored.append(copy.deepcopy(row))
    elif mutation == "target":
        row["outputs_json"] = json.dumps({"target_table": "foreign.table"})
    else:
        row[mutation] = "foreign"
    monkeypatch.setattr("fabric_etl_phase6.vault.rows",
                        lambda spark, query: [batch(release)] if "batch_context" in query else stored)
    with pytest.raises(ContractError, match="target differs" if mutation == "target" else "authoritative"):
        read_vault_commit(Control(), project=release, entity_id="HUB_CUSTOMER", table="phase6_hub_customer",
                          commit_id="current-commit", batch_id="current")
