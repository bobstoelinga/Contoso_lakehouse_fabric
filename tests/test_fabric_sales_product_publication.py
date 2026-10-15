from datetime import datetime, timezone
import copy
import json

import pytest

from fabric_etl.contracts import ContractError
from fabric_etl.control import literal
from fabric_etl_phase6.publication import PRODUCT_ID, publish_gold, validate_gold_commit
from test_fabric_sales_vault import project


def test_product_pointer_cannot_change_without_explicit_proof_scope():
    with pytest.raises(ContractError, match="explicit isolated"):
        publish_gold(None, None, project=project(), receipt={}, batch_id="batch", now=datetime.now(timezone.utc))


class Environment:
    def spark_table(self, role, schema, table):
        return schema + "." + table


class Control:
    project = "contoso"
    environment = Environment()
    spark = object()

    def assert_owner(self, owner):
        assert owner == "owner"

    def table(self, name):
        return name


def receipt():
    return {"commit": {"commit_id": "gold-commit"}, "product_release_id": "new",
            "input_commit_ids": ["raw", "bv"]}


def setup_pointer(monkeypatch):
    cutoff = datetime(2026, 10, 12, 6, 0)
    manifest = {"product_release_id": "new", "targets": {}}
    monkeypatch.setattr("fabric_etl_phase6.publication.validate_gold_commit",
                        lambda *args, **kwargs: {"manifest": manifest, "cutoff": cutoff})
    from fabric_etl.contracts import canonical_json

    registered = {
        "input_commits_json": canonical_json({"gold_commit_id": "gold-commit", "input_commit_ids": ["raw", "bv"]}).decode(),
        "entity_manifest_json": canonical_json(manifest).decode(), "cutoff": cutoff,
        "spark_validation": "VERIFIED", "sql_validation": "NOT_TESTED",
        "consumer_status": "PROOF_ONLY_NOT_PUBLISHED", "status": "COMMITTED",
    }
    return cutoff, registered


@pytest.mark.parametrize("change", ["duplicate", "manifest", "inputs", "cutoff", "sql_claim", "consumer_claim"])
def test_registered_product_release_cannot_mutate_or_claim_unproven_acceptance(monkeypatch, change):
    _, registered = setup_pointer(monkeypatch)
    stored = [copy.deepcopy(registered)]
    if change == "duplicate":
        stored.append(copy.deepcopy(registered))
    else:
        column = {"manifest": "entity_manifest_json", "inputs": "input_commits_json",
                  "cutoff": "cutoff", "sql_claim": "sql_validation", "consumer_claim": "consumer_status"}[change]
        stored[0][column] = "wrong"
    monkeypatch.setattr("fabric_etl_phase6.publication.rows", lambda *args: stored)
    with pytest.raises(ContractError, match="conflicts"):
        publish_gold(Control(), "owner", project=project(), receipt=receipt(), batch_id="batch",
                     now=datetime.now(timezone.utc), proof_only=True)


def test_published_retry_verifies_same_pointer_without_any_mutation(monkeypatch):
    _, registered = setup_pointer(monkeypatch)
    def read(spark, sql):
        return [{"active_release_id": "new", "generation": 3}] if "active_product" in sql else [registered]
    monkeypatch.setattr("fabric_etl_phase6.publication.rows", read)
    assert publish_gold(Control(), "owner", project=project(), receipt=receipt(), batch_id="batch",
                        now=datetime.now(timezone.utc), proof_only=True) == {
        "status": "VERIFIED", "active_release_id": "new", "generation": 3,
    }


def test_older_retry_preserves_newer_active_release_without_mutation(monkeypatch):
    _, registered = setup_pointer(monkeypatch)
    def read(spark, sql):
        if "active_product" in sql:
            return [{"active_release_id": "later", "generation": 3}]
        if literal("later") in sql:
            return [{"cutoff": datetime(2026, 10, 13), "status": "COMMITTED"}]
        return [registered]
    monkeypatch.setattr("fabric_etl_phase6.publication.rows", read)
    assert publish_gold(Control(), "owner", project=project(), receipt=receipt(), batch_id="batch",
                        now=datetime.now(timezone.utc), proof_only=True) == {
        "status": "VERIFIED_SUPERSEDED", "active_release_id": "later", "generation": 3,
    }


@pytest.mark.parametrize("change", ["duplicate_pointer", "missing_release", "uncommitted_release"])
def test_pointer_corruption_blocks_promotion_before_mutation(monkeypatch, change):
    _, registered = setup_pointer(monkeypatch)
    def read(spark, sql):
        if "active_product" in sql:
            pointer = {"active_release_id": "prior", "generation": 2}
            return [pointer, pointer] if change == "duplicate_pointer" else [pointer]
        if literal("prior") in sql:
            return [] if change == "missing_release" else [{"status": "BUILDING"}]
        return [registered]
    monkeypatch.setattr("fabric_etl_phase6.publication.rows", read)
    with pytest.raises(ContractError, match="duplicated|matching committed"):
        publish_gold(Control(), "owner", project=project(), receipt=receipt(), batch_id="batch",
                     now=datetime.now(timezone.utc), proof_only=True)


@pytest.mark.parametrize("previous", [None, "prior"])
def test_complete_release_is_registered_before_single_pointer_promotion(monkeypatch, previous):
    _, registered = setup_pointer(monkeypatch)
    statements = []

    class Spark:
        def sql(self, sql):
            statements.append(sql)
            return self

        def collect(self):
            return []

    control = Control()
    control.spark = Spark()

    def read(spark, sql):
        if "active_product" in sql:
            if any("active_product" in statement for statement in statements):
                return [{"active_release_id": "new", "generation": 3 if previous else 1}]
            return [{"active_release_id": "prior", "generation": 2}] if previous else []
        if literal("prior") in sql:
            return [{"cutoff": datetime(2026, 10, 11), "status": "COMMITTED"}]
        return [registered] if any("product_release" in statement for statement in statements) else []

    monkeypatch.setattr("fabric_etl_phase6.publication.rows", read)
    result = publish_gold(control, "owner", project=project(), receipt=receipt(), batch_id="batch",
                          now=datetime.now(timezone.utc), proof_only=True)
    assert result == {"status": "ACTIVATED", "active_release_id": "new", "generation": 3 if previous else 1}
    assert len(statements) == 2
    assert all(" VALUES " not in statement for statement in statements)
    assert statements[0].startswith("INSERT INTO publication.product_release SELECT ")
    assert statements[0].startswith("INSERT INTO publication.product_release")
    assert "'NOT_TESTED'" in statements[0]
    assert "'PROOF_ONLY_NOT_PUBLISHED'" in statements[0]
    assert "TIMESTAMP" in statements[0].upper()
    if previous:
        assert statements[1].startswith("UPDATE publication.active_product")
        assert "AND generation = 2" in statements[1]
    else:
        assert statements[1].startswith("INSERT INTO publication.active_product")
        assert " SELECT " in statements[1]


def test_failed_pointer_readback_is_an_error_not_success(monkeypatch):
    _, registered = setup_pointer(monkeypatch)
    class Spark:
        def sql(self, sql):
            return self
        def collect(self):
            return []
    control = Control()
    control.spark = Spark()
    def read(spark, sql):
        return [] if "active_product" in sql else [registered]
    monkeypatch.setattr("fabric_etl_phase6.publication.rows", read)
    with pytest.raises(ContractError, match="pointer readback differs"):
        publish_gold(control, "owner", project=project(), receipt=receipt(), batch_id="batch",
                     now=datetime.now(timezone.utc), proof_only=True)


@pytest.mark.parametrize("change", ["missing", "duplicate", "layer", "entity_id", "status", "batch_id", "release", "inventory"])
def test_publication_rechecks_exact_authoritative_gold_manifest_before_output_reads(monkeypatch, change):
    monkeypatch.setattr("fabric_etl_phase6.publication._proof_batch",
                        lambda *args: {"load_date": datetime(2026, 10, 12)})
    manifest = {"product_release_id": "new", "targets": {}}
    row = {"status": "COMMITTED", "layer": "GOLD", "entity_id": PRODUCT_ID, "batch_id": "batch",
           "outputs_json": json.dumps(manifest)}
    stored = [row]
    if change == "missing":
        stored = []
    elif change == "duplicate":
        stored.append(copy.deepcopy(row))
    elif change == "release":
        row["outputs_json"] = json.dumps({**manifest, "product_release_id": "foreign"})
    elif change != "inventory":
        row[change] = "foreign"
    monkeypatch.setattr("fabric_etl_phase6.publication.rows", lambda *args: stored)
    with pytest.raises(ContractError, match="authoritative|release identity|eleven-entity"):
        validate_gold_commit(Control(), project=project(), receipt=receipt(), batch_id="batch")
