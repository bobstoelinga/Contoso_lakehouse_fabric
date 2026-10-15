from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
import hashlib
import json

import pytest

from fabric_etl.context import BatchContext
from fabric_etl.contracts import ContractError, Environment, canonical_json, load_json
from fabric_etl.control import ControlStore, Owner, literal, utc
from fabric_etl.migrations import declarations, migrate, render
from fabric_etl.project import ProjectRelease
from fabric_etl.proof import candidate, hash_vectors, java_delta_conflict


ROOT = Path(__file__).resolve().parents[1]
NOW = datetime(2026, 10, 10, 8, tzinfo=timezone.utc)
OWNER = Owner("contoso", "owner-1", 1, "job-1")
HELD = {**asdict(OWNER), "state": "HELD"}
FREE = {"project_id": "contoso", "owner_id": None, "generation": 0,
        "job_instance_id": None, "state": "FREE"}


def env():
    return Environment.from_dict(load_json(ROOT / "environments" / "contoso.dev.json"))


class Row(dict):
    def asDict(self, recursive=False):
        return dict(self)


class QueueSpark:
    def __init__(self, reads):
        self.reads = list(reads)
        self.queries = []
    def sql(self, query):
        self.queries.append(query)
        result = self.reads.pop(0) if query.startswith("SELECT ") else []
        return SimpleNamespace(collect=lambda: [Row(row) for row in result])


def test_claim_is_conditional_and_reads_exact_owner_back(monkeypatch):
    monkeypatch.setattr("fabric_etl.control.uuid4", lambda: "owner-1")
    spark = QueueSpark([[FREE], [HELD]])
    assert ControlStore(spark, env()).claim("job-1", now=NOW) == OWNER
    assert "AND state = 'FREE' AND owner_id IS NULL AND generation = 0" in spark.queries[1]
    assert not any("INSERT" in query for query in spark.queries)


@pytest.mark.parametrize("initial", [[], [FREE, FREE], [HELD]])
def test_missing_duplicate_and_held_owner_fail_before_writes(initial):
    spark = QueueSpark([initial])
    with pytest.raises(ContractError):
        ControlStore(spark, env()).claim("job-2", now=NOW)
    assert all(query.startswith("SELECT") for query in spark.queries)


def test_claim_loser_stale_release_and_no_ttl_takeover(monkeypatch):
    monkeypatch.setattr("fabric_etl.control.uuid4", lambda: "loser")
    spark = QueueSpark([[FREE], [HELD]])
    with pytest.raises(ContractError, match="OWNERSHIP_LOST"):
        ControlStore(spark, env()).claim("job-2", now=NOW)
    spark = QueueSpark([[{**HELD, "generation": 2}]])
    with pytest.raises(ContractError, match="OWNERSHIP_LOST"):
        ControlStore(spark, env()).release(OWNER)
    assert all(query.startswith("SELECT") for query in spark.queries)


def test_bootstrap_requires_exclusive_setup_and_does_not_reset_existing_owner():
    spark = QueueSpark([])
    with pytest.raises(ContractError, match="exclusive"):
        ControlStore(spark, env()).bootstrap_owner(exclusive_setup=False)
    spark = QueueSpark([[HELD], [HELD]])
    with pytest.raises(ContractError, match="held"):
        ControlStore(spark, env()).bootstrap_owner(exclusive_setup=True)
    assert not any("INSERT" in query for query in spark.queries)


def test_owner_bootstrap_uses_scalar_projection_not_inline_values():
    spark = QueueSpark([[], [FREE]])
    ControlStore(spark, env()).bootstrap_owner(exclusive_setup=True)
    insert = next(query for query in spark.queries if query.startswith("INSERT"))
    assert ") SELECT " in insert and "VALUES" not in insert


def test_batch_retry_restores_persisted_time_not_new_wallclock():
    context = BatchContext.create("contoso", "SALES", "delivery-1", "release-1", load_date=NOW)
    row = {"batch_id": context.batch_id, "project_id": "contoso", "source_system_id": "SALES",
           "delivery_id": "delivery-1", "release_id": "release-1", "delivery_sequence": 1,
           "load_date": NOW.replace(tzinfo=None)}
    spark = QueueSpark([[HELD], [row], [HELD]])
    result = ControlStore(spark, env()).batch_context(OWNER, "SALES", "delivery-1", 1,
                                                    "release-1", now=NOW + timedelta(days=1))
    assert result == context
    assert not any("INSERT" in query for query in spark.queries)


def test_batch_clock_rollback_advances_one_microsecond():
    expected = NOW + timedelta(microseconds=1)
    spark = QueueSpark([
        [HELD], [], [{"delivery_sequence": 1, "load_date": NOW.replace(tzinfo=None)}],
        [HELD], [{"load_date": expected.replace(tzinfo=None)}],
    ])
    result = ControlStore(spark, env()).batch_context(
        OWNER, "PHASE4_PROOF", "delivery-2", 2, "release-1", now=NOW - timedelta(days=1),
        proof_only=True,
    )
    assert datetime.fromisoformat(result.load_date_utc) == expected


def test_unapproved_metadata_activation_is_rejected_before_writes():
    project = ProjectRelease.load(ROOT / "projects" / "contoso" / "release.json")
    spark = QueueSpark([[HELD]])
    with pytest.raises(ContractError, match="METADATA_ACTIVATION_BLOCKED"):
        ControlStore(spark, env()).activate(OWNER, project, {}, now=NOW)
    assert len(spark.queries) == 1


def test_candidate_persists_full_files_and_rejects_changed_content():
    project = ProjectRelease.load(ROOT / "projects" / "contoso" / "release.json")
    files = {path: (ROOT / "projects" / "contoso" / path).read_bytes().decode("utf-8")
             for path in project.document["files"]}
    persisted = {
        "document_json": canonical_json(project.document).decode(),
        "files_json": canonical_json(files).decode(), "status": "CANDIDATE",
    }
    spark = QueueSpark([[HELD], [], [HELD], [persisted]])
    assert ControlStore(spark, env()).register_candidate(OWNER, project, files, now=NOW) == project.release_id
    insert = next(query for query in spark.queries if query.startswith("INSERT"))
    assert ") SELECT " in insert and "VALUES" not in insert
    assert literal(persisted["files_json"]) in insert
    files[next(iter(files))] += "\nchanged"
    spark = QueueSpark([[HELD], []])
    with pytest.raises(ContractError, match="checksums"):
        ControlStore(spark, env()).register_candidate(OWNER, project, files, now=NOW)
    assert all(query.startswith("SELECT") for query in spark.queries)


def test_new_business_batch_requires_active_approved_metadata():
    spark = QueueSpark([[HELD], [], []])
    with pytest.raises(ContractError, match="active approved"):
        ControlStore(spark, env()).batch_context(OWNER, "SALES", "new", 3, "release-1", now=NOW)
    assert all(query.startswith("SELECT") for query in spark.queries)


def test_proof_context_cannot_write_a_business_source():
    spark = QueueSpark([[HELD], []])
    with pytest.raises(ContractError, match="isolated"):
        ControlStore(spark, env()).batch_context(OWNER, "SALES", "new", 3, "release-1",
                                                now=NOW, proof_only=True)


def test_sql_literals_unicode_quote_and_backslash_are_encoded_without_interpolation():
    assert literal("'\\é") == "decode(unhex('275cc3a9'), 'UTF-8')"
    with pytest.raises(ContractError):
        literal(None)
    with pytest.raises(ContractError):
        utc(datetime(2026, 1, 1))


@pytest.mark.parametrize("name,expected", [
    ("io.delta.exceptions.ConcurrentAppendException", "io.delta.exceptions.ConcurrentAppendException"),
    ("io.delta.exceptions.ConcurrentWriteException", "io.delta.exceptions.ConcurrentWriteException"),
    ("java.lang.SecurityException", None),
    ("io.delta.exceptions.ConcurrentDeleteDeleteException", None),
])
def test_java_conflicts_are_exactly_allowlisted_not_broadly_suppressed(name, expected):
    error = SimpleNamespace(java_exception=SimpleNamespace(
        getClass=lambda: SimpleNamespace(getName=lambda: name),
    ))
    assert java_delta_conflict(error) == expected
    assert java_delta_conflict(ValueError("unrelated")) is None


def migration_inputs():
    result = []
    for path in sorted((ROOT / "sql" / "migrations" / "fabric").glob("*.sql")):
        text = path.read_bytes().decode("utf-8")
        result.append({"id": path.stem, "source_sql": text,
                       "source_sha256": hashlib.sha256(text.encode()).hexdigest()})
    return result


def test_all_control_ddl_is_allowlisted_and_complete():
    statements = [declaration for migration in migration_inputs()
                  for declaration in declarations(env(), render(env(), migration["source_sql"]))]
    assert sum(bool(statement[1]) for statement in statements) == 12
    assert sum(not statement[1] for statement in statements) == 11
    assert all(column[1] in {"string", "int", "bigint", "timestamp"}
               for _, _, columns in statements for column in columns)


@pytest.mark.parametrize("sql", [
    "DROP TABLE anything", "INSERT INTO anything VALUES (1)",
    "CREATE SCHEMA IF NOT EXISTS unrelated",
    "CREATE TABLE IF NOT EXISTS other (id STRING) USING DELTA",
])
def test_destructive_or_unregistered_migrations_fail_before_execution(sql):
    with pytest.raises(ContractError):
        declarations(env(), sql)


def test_migration_checksum_and_exclusive_setup_fail_before_any_write():
    spark = QueueSpark([])
    with pytest.raises(ContractError, match="exclusive"):
        migrate(spark, env(), migration_inputs(), exclusive_setup=False, now=NOW)
    values = migration_inputs()
    values[0]["source_sql"] += "\n"
    with pytest.raises(ContractError, match="checksum"):
        migrate(spark, env(), values, exclusive_setup=True, now=NOW)
    assert not spark.queries


def test_portable_candidate_preserves_file_inventory_and_hashes(tmp_path):
    project = load_json(ROOT / "projects" / "contoso" / "release.json")
    files = {relative: (ROOT / "projects" / "contoso").joinpath(*relative.split("/")).read_bytes().decode()
             for relative in project["files"]}
    result = candidate(json.dumps(project), json.dumps(files), tmp_path)
    assert result.release_id == project["release_id"]
    files[next(iter(files))] += "changed"
    with pytest.raises(ContractError, match="fingerprint"):
        candidate(json.dumps(project), json.dumps(files), tmp_path)


def test_hash_proof_uses_contract_serialization_and_detects_mismatch():
    spark = QueueSpark([[{"hash": "wrong"}]])
    with pytest.raises(ContractError, match="hash vector"):
        hash_vectors(spark)
