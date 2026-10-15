"""Phase-four proofs only; no business ingestion or ETL replay."""

from __future__ import annotations

import json
import tempfile
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fabric_etl import __version__
from fabric_etl.contracts import ContractError, Environment, hash_key, parse_json
from fabric_etl.control import ControlSpark, ControlStore, literal, rows
from fabric_etl.control import Owner
from fabric_etl.migrations import migrate
from fabric_etl.project import ProjectRelease, safe_file
from fabric_etl.canonical import scalar, spark_scalar
from datetime import date
from decimal import Decimal
import time


def java_delta_conflict(error: object) -> str | None:
    java_error = getattr(error, "java_exception", None)
    if java_error is None:
        return None
    name = java_error.getClass().getName()
    if name in {
        "io.delta.exceptions.ConcurrentAppendException",
        "io.delta.exceptions.ConcurrentWriteException",
    }:
        return name
    return None


def hash_vectors(spark: ControlSpark) -> list[dict]:
    vectors = [[None], [""], [" a "], ["a||b", "c"], ["a", "b||c"], ["é"], ["^^", None]]
    result = []
    for values in vectors:
        normalized = [value.strip().upper() if value is not None else None for value in values]
        # Match the contract's UTF-8 JSON encoding, including NULL elements.
        encoded = json.dumps(["fabric-dv-key-v1", "PROOF", normalized],
                             ensure_ascii=False, separators=(",", ":"))
        expected = hash_key("PROOF", values)
        actual = rows(spark, f"SELECT sha2({literal(encoded)}, 256) AS hash")[0]["hash"]
        if expected != actual:
            raise ContractError("Python/Spark hash vector differs")
        result.append({"values": values, "hash": actual})
    return result


def type_vectors(spark: ControlSpark) -> list[dict]:
    timezone_row = rows(spark, "SET spark.sql.session.timeZone")[0]
    if timezone_row.get("value") != "UTC":
        raise ContractError("Spark session timezone must be UTC")
    vectors = [
        ("STRING", " a ", "' a '"), ("STRING", None, "CAST(NULL AS STRING)"),
        ("BOOLEAN", True, "true"), ("LONG", 9223372036854775807, "CAST(9223372036854775807 AS BIGINT)"),
        ("DECIMAL(38,18)", Decimal("99999999999999999999.999999999999999999"),
         "CAST('99999999999999999999.999999999999999999' AS DECIMAL(38,18))"),
        ("DECIMAL(18,4)", Decimal("-0.0000"), "CAST('-0.0000' AS DECIMAL(18,4))"),
        ("DATE", date(2026, 10, 10), "DATE '2026-10-10'"),
        ("TIMESTAMP", datetime(2026, 10, 25, 2, 30, 0, 123456, tzinfo=timezone(timedelta(hours=2))),
         "TIMESTAMP '2026-10-25 00:30:00.123456'"),
        ("TIMESTAMP", datetime(2026, 10, 25, 2, 30, 0, 123456, tzinfo=timezone(timedelta(hours=1))),
         "TIMESTAMP '2026-10-25 01:30:00.123456'"),
    ]
    results = []
    for kind, value, expression in vectors:
        projected = spark_scalar("value", kind)
        canonical = scalar(value, kind)
        prefix = "to_json(array('fabric-dv-key-v1','TYPE_PROOF'))"
        payload = f"concat(substring({prefix}, 1, length({prefix}) - 1), ',', to_json(array({projected})), ']')"
        actual = rows(spark, f"SELECT {projected} AS canonical, {payload} AS payload, "
                      f"sha2({payload}, 256) AS hash FROM (SELECT {expression} AS value)")[0]
        expected_payload = json.dumps(["fabric-dv-key-v1", "TYPE_PROOF", [canonical]],
                                      ensure_ascii=False, separators=(",", ":"))
        if (actual["canonical"] != canonical or actual["payload"] != expected_payload
                or actual["hash"] != hash_key("TYPE_PROOF", [canonical])):
            raise ContractError(f"Python/Spark type/hash vector differs: {kind}")
        results.append({"data_type": kind, **actual})
    return results


def candidate(project_json: str, files_json: str, root: Path) -> ProjectRelease:
    document = parse_json(project_json)
    files = parse_json(files_json)
    if not isinstance(document, dict) or not isinstance(files, dict) or set(files) != set(document["files"]):
        raise ContractError("Project file payload inventory differs")
    for relative, text in files.items():
        if not isinstance(relative, str) or not isinstance(text, str):
            raise ContractError("Project file payloads must be text")
        # Apply the same path grammar before creating the file in the isolated driver folder.
        import re
        if not re.fullmatch(r"[A-Za-z0-9_.-]+(?:/[A-Za-z0-9_.-]+)*", relative):
            raise ContractError("Unsafe candidate payload path")
        if any(segment in {".", ".."} for segment in relative.split("/")):
            raise ContractError("Unsafe candidate payload path")
        destination = root.joinpath(*relative.split("/"))
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(text.encode("utf-8"))
        safe_file(root, relative)
    return ProjectRelease.validate(document, root)


def setup_proof(
    spark: ControlSpark, environment_json: str, expected_version: str,
    migrations_json: str, project_json: str, files_json: str, activity_id: str,
) -> dict:
    if expected_version != __version__:
        raise ContractError("Installed version mismatch")
    environment = Environment.from_dict(parse_json(environment_json))
    now = datetime.now(timezone.utc)
    with tempfile.TemporaryDirectory(prefix="fabric-project-") as directory:
        project = candidate(project_json, files_json, Path(directory))
        applied = migrate(spark, environment, parse_json(migrations_json),
                          exclusive_setup=True, now=now)
        store = ControlStore(spark, environment)
        store.bootstrap_owner(exclusive_setup=True)
        owner = store.claim(activity_id, now=now)
        try:
            file_payloads = parse_json(files_json)
            store.register_candidate(owner, project, file_payloads, now=now)
            try:
                store.activate(owner, project, file_payloads, now=now)
            except ContractError as exc:
                if not str(exc).startswith("METADATA_ACTIVATION_BLOCKED:"):
                    raise
                activation = "BLOCKED_AS_EXPECTED"
            else:
                raise ContractError("Unapproved project unexpectedly activated")
            first = store.batch_context(owner, "PHASE4_PROOF", "retry-vector-1", 1,
                                        project.release_id, now=now, proof_only=True)
            retry = store.batch_context(owner, "PHASE4_PROOF", "retry-vector-1", 1,
                                        project.release_id, now=now + timedelta(days=1), proof_only=True)
            second = store.batch_context(owner, "PHASE4_PROOF", "retry-vector-2", 2,
                                         project.release_id, now=datetime(2000, 1, 1, tzinfo=timezone.utc),
                                         proof_only=True)
            if first != retry or second.load_date_utc <= first.load_date_utc:
                raise ContractError("Durable retry or monotonic batch time proof failed")
            store.heartbeat(owner, now=now)
            event = store.audit(owner, activity_id, "PHASE4_CONTROL_PROOF", now=now)
            hashes = hash_vectors(spark)
            types = type_vectors(spark)
        finally:
            # Release only this known owner. A Spark failure is still propagated to the job.
            store.release(owner)
    return {
        "status": "SUCCEEDED", "scope": "PHASE4_CONTROL_PROOF_NO_BUSINESS_INGESTION",
        "framework_version": __version__, "migrations": applied,
        "project_release_id": project.release_id, "candidate_activation": activation,
        "runtime_blocker_count": len(project.runtime_blockers),
        "batch_retry_identical": True, "load_time_monotonic": True,
        "first_batch": asdict(first), "second_batch": asdict(second),
        "audit_event_id": event, "hash_vectors": hashes, "type_vectors": types,
        "owner_released": True,
    }


def owner_probe(spark: ControlSpark, environment_json: str, activity_id: str,
                mode: str, start_utc: str, owner_json: str, terminal_proof_json: str) -> dict:
    environment = Environment.from_dict(parse_json(environment_json))
    store = ControlStore(spark, environment)
    if mode == "inspect":
        row = store.owner_row()
        return {"status": "SUCCEEDED", "scope": "READ_ONLY_OWNER_INSPECTION",
                "owner": {key: row[key] for key in ("project_id", "owner_id", "generation", "job_instance_id")},
                "owner_state": row["state"]}
    if mode == "recover":
        owner = Owner(**parse_json(owner_json))
        proof = parse_json(terminal_proof_json)
        if (proof.get("workspace_id") != environment.workspace_id
                or proof.get("livy_id") != owner.job_instance_id
                or proof.get("job_status") not in {"Failed", "Completed", "Cancelled"}
                or proof.get("livy_state") not in {"Failed", "Succeeded", "Cancelled", "Completed"}):
            raise ContractError("Recovery requires an explicitly verified terminal job and Spark session")
        store.assert_owner(owner)
        event = store.audit(owner, activity_id, "CONFIRMED_STOPPED_WRITER_RECOVERY",
                            now=datetime.now(timezone.utc))
        store.release(owner)
        return {"status": "SUCCEEDED", "scope": "EXPLICIT_STOPPED_WRITER_RECOVERY",
                "recovered_owner": asdict(owner), "terminal_proof": proof, "audit_event_id": event}
    if mode == "probe":
        try:
            owner = store.claim(activity_id, now=datetime.now(timezone.utc))
        except ContractError as exc:
            if not str(exc).startswith("PROJECT_BUSY:"):
                raise
            return {"status": "BLOCKED", "scope": "OWNER_NO_TTL_TAKEOVER", "reason": str(exc),
                    "observed_owner": store.owner_row()["owner_id"]}
        else:
            store.release(owner)
            raise ContractError("Expected held writer was incorrectly available")
    if mode == "stop":
        owner = store.claim(activity_id, now=datetime.now(timezone.utc))
        store.audit(owner, activity_id, "INTENTIONAL_PHASE4_WRITER_STOP", now=datetime.now(timezone.utc))
        # Deliberately do not release: this fixture proves explicit stopped-writer recovery.
        raise RuntimeError("INTENTIONAL_PHASE4_WRITER_STOP: owner retained for verified recovery")
    if mode != "race":
        raise ContractError("Unsupported owner proof mode")
    from delta.exceptions import ConcurrentAppendException, ConcurrentWriteException
    from py4j.protocol import Py4JJavaError
    current = store.owner_row()
    if current["state"] != "FREE" or current["owner_id"] is not None:
        raise ContractError("Race requires a FREE owner at the read barrier")
    barrier = datetime.fromisoformat(start_utc)
    if barrier.tzinfo is None:
        raise ContractError("Race barrier must be timezone-aware")
    remaining = (barrier - datetime.now(timezone.utc)).total_seconds()
    if remaining <= 0 or remaining > 600:
        raise ContractError("Race missed its read-before-write barrier")
    time.sleep(remaining)
    try:
        owner = store._claim_generation(activity_id, current["generation"], now=datetime.now(timezone.utc))
    except (ConcurrentAppendException, ConcurrentWriteException) as exc:
        return {"status": "BLOCKED", "scope": "OWNER_RACE", "reason": type(exc).__name__,
                "ready_generation": current["generation"], "activity_id": activity_id}
    except Py4JJavaError as exc:
        conflict = java_delta_conflict(exc)
        if conflict is None:
            raise
        return {"status": "BLOCKED", "scope": "OWNER_RACE", "reason": conflict,
                "ready_generation": current["generation"], "activity_id": activity_id}
    except ContractError as exc:
        if not str(exc).startswith("OWNERSHIP_LOST:"):
            raise
        return {"status": "BLOCKED", "scope": "OWNER_RACE", "reason": str(exc),
                "ready_generation": current["generation"], "activity_id": activity_id}
    try:
        event = store.audit(owner, activity_id, "OWNER_RACE_WINNER", now=datetime.now(timezone.utc))
        for _ in range(4):
            time.sleep(15)
            store.heartbeat(owner, now=datetime.now(timezone.utc))
    finally:
        store.release(owner)
    return {"status": "SUCCEEDED", "scope": "OWNER_RACE", "owner": asdict(owner),
            "ready_generation": current["generation"], "audit_event_id": event}
