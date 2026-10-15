import copy
from pathlib import Path

import pytest

from fabric_etl.contracts import ContractError
from fabric_etl.project import ProjectRelease
from fabric_etl_phase6.quality import committed_output, quality_order, required_references


PROJECT = Path(__file__).resolve().parents[1] / "projects" / "contoso" / "release.json"


def test_sales_quality_order_includes_every_required_reference_before_use():
    project = ProjectRelease.load(PROJECT)
    sources = {source["id"] for source in project.document["source_objects"] if source["source_system_id"] == "SALES"}
    ordered = quality_order(project, sources)
    assert set(ordered) == sources
    assert len(ordered) == 5
    for source in ordered:
        assert all(ordered.index(parent) < ordered.index(source) for parent in required_references(project, source))
    assert ordered.index("SALES.CUSTOMERS") < ordered.index("SALES.ORDERS")
    assert set(required_references(project, "SALES.RETURNS")) == {"SALES.EMPLOYEES", "SALES.PRODUCTS"}


def test_quality_scope_cannot_omit_references():
    with pytest.raises(ContractError, match="omits a required"):
        quality_order(ProjectRelease.load(PROJECT), {"SALES.ORDERS"})
    with pytest.raises(ContractError, match="nonempty"):
        quality_order(ProjectRelease.load(PROJECT), set())


def test_quality_reference_cycle_fails_instead_of_skipping(monkeypatch):
    monkeypatch.setattr("fabric_etl_phase6.quality.required_references",
                        lambda project, source: ["B"] if source == "A" else ["A"])
    with pytest.raises(ContractError, match="cycle"):
        quality_order(ProjectRelease.load(PROJECT), {"A", "B"})


class Environment:
    def spark_table(self, role, schema, table):
        return f"{role}.{schema}.{table}"


class Control:
    project = "contoso"
    environment = Environment()
    spark = object()

    def table(self, name):
        return name


def test_committed_reference_rejects_foreign_target_before_read():
    with pytest.raises(ContractError, match="explicit binding"):
        committed_output(
            Control(), commit_id="commit", batch_id="batch", layer="QUALITY", entity="SALES.CUSTOMERS",
            source_id="SALES.CUSTOMERS", target="foreign.target", role="processing", schema="quality",
            table="phase6_customers", source_system_id="PHASE4_PROOF", delivery_id="delivery", release_id="release",
        )


@pytest.mark.parametrize("mutation", ["missing", "duplicate", "batch_id", "layer", "entity_id", "status"])
def test_committed_reference_rejects_non_authoritative_manifest(monkeypatch, mutation):
    row = {"batch_id": "batch", "layer": "QUALITY", "entity_id": "SALES.CUSTOMERS", "status": "COMMITTED"}
    stored = [copy.deepcopy(row)]
    if mutation == "missing":
        stored = []
    elif mutation == "duplicate":
        stored.append(copy.deepcopy(row))
    else:
        stored[0][mutation] = "wrong"
    monkeypatch.setattr("fabric_etl_phase6.quality.rows", lambda *args: stored)
    with pytest.raises(ContractError, match="authoritative"):
        committed_output(
            Control(), commit_id="commit", batch_id="batch", layer="QUALITY", entity="SALES.CUSTOMERS",
            source_id="SALES.CUSTOMERS", target="processing.quality.phase6_customers",
            role="processing", schema="quality", table="phase6_customers",
            source_system_id="PHASE4_PROOF", delivery_id="delivery", release_id="release",
        )
