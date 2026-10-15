"""Durable control operations; all runtime writes require a project owner."""

from __future__ import annotations

import json
import hashlib
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Protocol
from uuid import uuid4

from fabric_etl.context import BatchContext
from fabric_etl.contracts import ContractError, Environment, canonical_json, fingerprint, identifier
from fabric_etl.project import ProjectRelease


class ControlSpark(Protocol):
    def sql(self, query: str) -> Any: ...
    def table(self, name: str) -> Any: ...


def literal(value: str) -> str:
    if not isinstance(value, str):
        raise ContractError("Control SQL literals must be strings")
    return f"decode(unhex('{value.encode('utf-8').hex()}'), 'UTF-8')"


def utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ContractError("Control timestamp must be timezone-aware")
    return value.astimezone(timezone.utc)


def timestamp(value: datetime) -> str:
    return "TIMESTAMP '" + utc(value).strftime("%Y-%m-%d %H:%M:%S.%f") + "'"


def rows(spark: ControlSpark, query: str) -> list[dict]:
    return [row.asDict(recursive=True) for row in spark.sql(query).collect()]


@dataclass(frozen=True)
class Owner:
    project_id: str
    owner_id: str
    generation: int
    job_instance_id: str


class ControlStore:
    def __init__(self, spark: ControlSpark, environment: Environment) -> None:
        environment.require_provisioned()
        self.spark = spark
        self.environment = environment
        self.project = environment.project_id

    def table(self, name: str) -> str:
        return self.environment.spark_table("control", "control", name)

    def owner_row(self) -> dict:
        result = rows(self.spark, f"SELECT * FROM {self.table('project_owner')} "
                      f"WHERE project_id = {literal(self.project)}")
        if len(result) != 1:
            raise ContractError("Owner cardinality must be exactly one; exclusive bootstrap/recovery required")
        return result[0]

    def bootstrap_owner(self, *, exclusive_setup: bool) -> None:
        if exclusive_setup is not True:
            raise ContractError("Owner bootstrap requires exclusive foundation setup")
        result = rows(self.spark, f"SELECT * FROM {self.table('project_owner')} "
                      f"WHERE project_id = {literal(self.project)}")
        if len(result) > 1:
            raise ContractError("Duplicate project owner rows")
        if not result:
            self.spark.sql(
                f"INSERT INTO {self.table('project_owner')} "
                "(project_id, owner_id, generation, state, heartbeat, job_instance_id) "
                f"SELECT {literal(self.project)}, NULL, 0, 'FREE', NULL, NULL"
            ).collect()
        current = self.owner_row()
        if current["state"] != "FREE" or current["owner_id"] is not None:
            raise ContractError("Bootstrap cannot replace a held owner")

    def claim(self, job_instance_id: str, *, now: datetime) -> Owner:
        if not job_instance_id or not isinstance(job_instance_id, str):
            raise ContractError("Job instance identity required")
        current = self.owner_row()
        if current["state"] != "FREE" or current["owner_id"] is not None:
            raise ContractError("PROJECT_BUSY: writer already held; no TTL takeover")
        generation = current["generation"]
        return self._claim_generation(job_instance_id, generation, now=now)

    def _claim_generation(self, job_instance_id: str, generation: int, *, now: datetime) -> Owner:
        if not isinstance(job_instance_id, str) or not job_instance_id:
            raise ContractError("Job instance identity required")
        if type(generation) is not int or generation < 0:
            raise ContractError("Invalid persisted owner generation")
        owner = Owner(self.project, str(uuid4()), generation + 1, job_instance_id)
        self.spark.sql(
            f"UPDATE {self.table('project_owner')} SET state = 'HELD', "
            f"owner_id = {literal(owner.owner_id)}, generation = {owner.generation}, "
            f"heartbeat = {timestamp(now)}, job_instance_id = {literal(job_instance_id)} "
            f"WHERE project_id = {literal(self.project)} AND state = 'FREE' "
            f"AND owner_id IS NULL AND generation = {generation}"
        ).collect()
        self.assert_owner(owner)
        return owner

    def assert_owner(self, owner: Owner) -> None:
        current = self.owner_row()
        if (
            owner.project_id != self.project or current["state"] != "HELD"
            or current["owner_id"] != owner.owner_id
            or current["generation"] != owner.generation
            or current["job_instance_id"] != owner.job_instance_id
        ):
            raise ContractError("OWNERSHIP_LOST: runtime writes prohibited")

    def heartbeat(self, owner: Owner, *, now: datetime) -> None:
        self.assert_owner(owner)
        self.spark.sql(
            f"UPDATE {self.table('project_owner')} SET heartbeat = {timestamp(now)} "
            f"WHERE project_id = {literal(self.project)} AND owner_id = {literal(owner.owner_id)} "
            f"AND generation = {owner.generation} AND state = 'HELD'"
        ).collect()
        self.assert_owner(owner)

    def release(self, owner: Owner) -> None:
        self.assert_owner(owner)
        self.spark.sql(
            f"UPDATE {self.table('project_owner')} SET state = 'FREE', owner_id = NULL, "
            f"job_instance_id = NULL, heartbeat = NULL WHERE project_id = {literal(self.project)} "
            f"AND owner_id = {literal(owner.owner_id)} AND generation = {owner.generation} "
            "AND state = 'HELD'"
        ).collect()
        current = self.owner_row()
        if current["state"] != "FREE" or current["owner_id"] is not None:
            raise ContractError("Owner release did not complete")

    def register_candidate(
        self, owner: Owner, project: ProjectRelease, files: dict[str, str], *, now: datetime,
    ) -> str:
        self.assert_owner(owner)
        if project.document["project_id"] != self.project:
            raise ContractError("Metadata project binding mismatch")
        table = self.environment.spark_table("control", "metadata", "project_release")
        condition = f"project_id = {literal(self.project)} AND release_id = {literal(project.release_id)}"
        existing = rows(self.spark, f"SELECT * FROM {table} WHERE {condition}")
        content = canonical_json(project.document).decode()
        if set(files) != set(project.document["files"]) or any(
            not isinstance(text, str) or hashlib.sha256(text.encode()).hexdigest() != project.document["files"][path]
            for path, text in files.items()
        ):
            raise ContractError("Metadata file payload inventory/checksums differ")
        file_payload = canonical_json(files).decode()
        if existing:
            if len(existing) != 1 or existing[0]["document_json"] != content or existing[0]["files_json"] != file_payload:
                raise ContractError("Conflicting or duplicate metadata candidate")
        else:
            self.spark.sql(
                f"INSERT INTO {table} (project_id, release_id, contract_version, document_json, "
                "files_json, status, registered_at, validated_at) SELECT "
                f"{literal(self.project)}, {literal(project.release_id)}, '1.0', {literal(content)}, "
                f"{literal(file_payload)}, 'CANDIDATE', {timestamp(now)}, {timestamp(now)}"
            ).collect()
        self.assert_owner(owner)
        verified = rows(self.spark, f"SELECT document_json, files_json, status FROM {table} WHERE {condition}")
        if len(verified) != 1 or verified[0]["document_json"] != content or verified[0]["files_json"] != file_payload:
            raise ContractError("Metadata candidate readback differs")
        return project.release_id

    def activate(
        self, owner: Owner, project: ProjectRelease, files: dict[str, str], *, now: datetime,
    ) -> None:
        self.assert_owner(owner)
        if project.runtime_blockers:
            raise ContractError("METADATA_ACTIVATION_BLOCKED: " + "; ".join(project.runtime_blockers))
        self.register_candidate(owner, project, files, now=now)
        release_table = self.environment.spark_table("control", "metadata", "project_release")
        release_condition = f"project_id = {literal(self.project)} AND release_id = {literal(project.release_id)}"
        self.spark.sql(
            f"UPDATE {release_table} SET status = 'VALIDATED' WHERE {release_condition}"
        ).collect()
        validated = rows(self.spark, f"SELECT status FROM {release_table} WHERE {release_condition}")
        if len(validated) != 1 or validated[0]["status"] != "VALIDATED":
            raise ContractError("Metadata release validation state did not persist")
        table = self.table("metadata_pointer")
        existing = rows(self.spark, f"SELECT * FROM {table} WHERE project_id = {literal(self.project)}")
        if len(existing) > 1:
            raise ContractError("Duplicate metadata pointer")
        if existing and existing[0]["active_release_id"] == project.release_id:
            self.assert_owner(owner)
            return
        generation = existing[0]["generation"] + 1 if existing else 1
        if existing:
            self.spark.sql(
                f"UPDATE {table} SET active_release_id = {literal(project.release_id)}, "
                f"generation = {generation}, updated_at = {timestamp(now)} "
                f"WHERE project_id = {literal(self.project)} AND generation = {generation - 1}"
            ).collect()
        else:
            self.spark.sql(
                f"INSERT INTO {table} SELECT {literal(self.project)}, "
                f"{literal(project.release_id)}, {generation}, {timestamp(now)}"
            ).collect()
        self.assert_owner(owner)
        result = rows(self.spark, f"SELECT * FROM {table} WHERE project_id = {literal(self.project)}")
        if len(result) != 1 or result[0]["active_release_id"] != project.release_id:
            raise ContractError("Metadata activation readback failed")

    def batch_context(
        self, owner: Owner, source: str, delivery: str, sequence: int, release_id: str,
        *, now: datetime, proof_only: bool = False,
    ) -> BatchContext:
        self.assert_owner(owner)
        identifier(source)
        if type(sequence) is not int or sequence < 1:
            raise ContractError("Delivery sequence must be positive")
        candidate = BatchContext.create(self.project, source, delivery, release_id, load_date=utc(now))
        table = self.table("batch_context")
        existing = rows(self.spark, f"SELECT * FROM {table} WHERE batch_id = {literal(candidate.batch_id)}")
        if existing:
            if len(existing) != 1 or existing[0]["delivery_sequence"] != sequence:
                raise ContractError("Conflicting persisted batch context")
            row = existing[0]
            loaded = row["load_date"]
            # Spark timestamps are UTC-naive when the session timezone is UTC.
            loaded = loaded.replace(tzinfo=timezone.utc) if loaded.tzinfo is None else utc(loaded)
            context = BatchContext.restore({
                "project_id": row["project_id"], "source_system_id": row["source_system_id"],
                "delivery_id": row["delivery_id"], "metadata_release_id": row["release_id"],
                "batch_id": row["batch_id"], "load_date_utc": loaded.isoformat(timespec="microseconds"),
            })
            self.assert_owner(owner)
            return context
        if proof_only:
            if source != "PHASE4_PROOF":
                raise ContractError("Proof contexts must use the isolated PHASE4_PROOF source")
        else:
            active = rows(self.spark, f"SELECT * FROM {self.table('metadata_pointer')} "
                          f"WHERE project_id = {literal(self.project)}")
            if len(active) != 1 or active[0]["active_release_id"] != release_id:
                raise ContractError("New business batch requires the active approved metadata release")
        previous = rows(self.spark, f"SELECT delivery_sequence, load_date FROM {table} "
                        f"WHERE project_id = {literal(self.project)} AND source_system_id = {literal(source)} "
                        "ORDER BY delivery_sequence DESC LIMIT 1")
        load_date = utc(now)
        if previous:
            if previous[0]["delivery_sequence"] >= sequence:
                raise ContractError("Out-of-order historical batch registration")
            earlier = previous[0]["load_date"]
            earlier = earlier.replace(tzinfo=timezone.utc) if earlier.tzinfo is None else utc(earlier)
            load_date = max(load_date, earlier + timedelta(microseconds=1))
        context = BatchContext.create(self.project, source, delivery, release_id, load_date=load_date)
        self.spark.sql(
            f"INSERT INTO {table} (batch_id, project_id, source_system_id, delivery_id, delivery_sequence, "
            "release_id, load_date, registered_at, source_time) SELECT "
            f"{literal(context.batch_id)}, {literal(self.project)}, {literal(source)}, {literal(delivery)}, "
            f"{sequence}, {literal(release_id)}, {timestamp(load_date)}, {timestamp(now)}, NULL"
        ).collect()
        self.assert_owner(owner)
        readback = rows(self.spark, f"SELECT * FROM {table} WHERE batch_id = {literal(context.batch_id)}")
        if len(readback) != 1:
            raise ContractError("Batch persistence cardinality mismatch")
        stored = readback[0]["load_date"]
        stored = stored.replace(tzinfo=timezone.utc) if stored.tzinfo is None else utc(stored)
        if stored != load_date:
            raise ContractError("Batch persistence timestamp differs")
        return context

    def audit(self, owner: Owner, correlation_id: str, transition: str, *, now: datetime) -> str:
        self.assert_owner(owner)
        event_id = fingerprint([owner.owner_id, owner.generation, correlation_id, transition])
        table = self.environment.spark_table("control", "audit", "events")
        existing = rows(self.spark, f"SELECT * FROM {table} WHERE event_id = {literal(event_id)}")
        content = json.dumps(asdict(owner), sort_keys=True)
        if existing and (len(existing) != 1 or existing[0]["correlation_id"] != correlation_id
                         or existing[0]["transition"] != transition or existing[0]["context_json"] != content):
            raise ContractError("Conflicting or duplicate audit event")
        if not existing:
            self.spark.sql(
                f"INSERT INTO {table} SELECT {literal(event_id)}, {literal(self.project)}, NULL, "
                f"{literal(correlation_id)}, NULL, NULL, 1, {literal(transition)}, "
                f"{timestamp(now)}, NULL, {literal(content)}"
            ).collect()
        self.assert_owner(owner)
        readback = rows(self.spark, f"SELECT * FROM {table} WHERE event_id = {literal(event_id)}")
        if len(readback) != 1 or readback[0]["context_json"] != content:
            raise ContractError("Audit event persistence readback failed")
        return event_id
