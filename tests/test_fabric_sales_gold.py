from datetime import datetime, timezone
from pathlib import Path

import pytest

from fabric_etl.contracts import ContractError
from fabric_etl_phase6.gold import bind_gold_sql, gold_columns, gold_sql, sales_gold_entities, write_gold
from test_fabric_sales_vault import project


ROOT = Path(__file__).resolve().parents[1] / "projects" / "contoso"


def sources():
    return {entity["expression_ref"]: (ROOT / "sql" / (entity["expression_ref"] + ".sql")).read_bytes().decode()
            for entity in project().document["gold_entities"] if entity["source_system_id"] == "SALES"}


def test_sales_gold_exact_product_scope_has_five_historical_and_six_current_entities():
    entities = sales_gold_entities(project())
    assert len(entities) == 11
    assert [entity["layer"] for entity in entities] == ["HISTORICAL"] * 5 + ["CURRENT"] * 6


def test_gold_runtime_audit_fields_are_explicit_not_dropped_or_guessed_from_sql():
    release = project()
    for entity in sales_gold_entities(release):
        business = gold_columns(release, entity, business_only=True)
        all_columns = gold_columns(release, entity, business_only=False)
        assert all(not column["name"].startswith("_") for column in business)
        assert len(all_columns) - len(business) == (2 if entity["layer"] == "HISTORICAL" else 3)
    entity = sales_gold_entities(release)[0]
    schema = next(schema for schema in release.document["schemas"] if schema["id"] == entity["schema_id"])
    schema["columns"].append({"name": "_foreign", "data_type": "STRING"})
    with pytest.raises(ContractError, match="runtime audit columns"):
        gold_columns(release, entity, business_only=True)


def test_reviewed_gold_overlay_uses_half_open_as_of_joins_not_latest_satellites():
    queries = gold_sql(project(), sources())
    query = queries["GH_FCT_SALES"]
    assert "AND bv.is_current" not in query
    assert "AND so.is_current" not in query
    assert "sol.load_date >= bv.load_date" in query
    assert "sol.load_date >= so.load_date" in query
    assert "sol.load_date < coalesce(bv.load_end_date" in query
    assert "loe.load_date <= sol.load_date" in query
    assert "lre.load_date <= sr.load_date" in queries["GH_FCT_RETURNS"]
    assert queries["GC_DIM_DATE"] == sources()["GC_DIM_DATE"]


@pytest.mark.parametrize("change", ["missing", "extra", "checksum"])
def test_gold_never_executes_missing_foreign_or_unpinned_sql(change):
    sql = sources()
    if change == "missing":
        del sql["GH_FCT_SALES"]
    elif change == "extra":
        sql["FOREIGN"] = "SELECT 1"
    else:
        sql["GH_FCT_SALES"] += " "
    with pytest.raises(ContractError):
        gold_sql(project(), sql)


def test_gold_binding_reads_only_explicit_committed_views():
    sql = "SELECT * FROM {processing}.raw_vault.hub_customer"
    assert bind_gold_sql(sql, {("processing", "raw_vault", "hub_customer"): "committed_customer"}) == (
        "SELECT * FROM `committed_customer`"
    )
    with pytest.raises(ContractError, match="unavailable"):
        bind_gold_sql(sql, {})
    with pytest.raises(ContractError, match="unresolved"):
        bind_gold_sql("SELECT * FROM {foreign}.table", {})


def test_gold_candidate_cannot_write_or_publish_without_explicit_proof_scope():
    with pytest.raises(ContractError, match="explicit isolated"):
        write_gold(None, None, project=project(), delivery={}, sources={}, now=datetime.now(timezone.utc))
