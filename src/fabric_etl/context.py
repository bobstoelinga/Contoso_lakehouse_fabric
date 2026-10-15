"""Immutable batch identity; persistence is the control adapter's responsibility."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import NAMESPACE_URL, uuid5

from fabric_etl.contracts import ContractError, canonical_json, fields, identifier, required_text


@dataclass(frozen=True)
class BatchContext:
    project_id: str
    source_system_id: str
    delivery_id: str
    metadata_release_id: str
    batch_id: str
    load_date_utc: str

    @classmethod
    def create(
        cls, project_id: str, source_system_id: str, delivery_id: str,
        metadata_release_id: str, *, load_date: datetime,
    ) -> BatchContext:
        identifier(project_id)
        identifier(source_system_id)
        required_text(delivery_id, "delivery_id")
        required_text(metadata_release_id, "metadata_release_id")
        if load_date.tzinfo is None or load_date.utcoffset() is None:
            raise ContractError("Batch time must be timezone-aware")
        identity = canonical_json([
            "fabric-batch-v1", project_id, source_system_id,
            delivery_id, metadata_release_id,
        ]).decode("utf-8")
        return cls(
            project_id, source_system_id, delivery_id, metadata_release_id,
            str(uuid5(NAMESPACE_URL, identity)),
            load_date.astimezone(timezone.utc).isoformat(timespec="microseconds"),
        )

    @classmethod
    def restore(cls, value: object) -> BatchContext:
        data = fields(value, {
            "project_id", "source_system_id", "delivery_id",
            "metadata_release_id", "batch_id", "load_date_utc",
        }, "Batch context")
        timestamp = required_text(data["load_date_utc"], "load_date_utc")
        context = cls.create(
            required_text(data["project_id"], "project_id"),
            required_text(data["source_system_id"], "source_system_id"),
            required_text(data["delivery_id"], "delivery_id"),
            required_text(data["metadata_release_id"], "metadata_release_id"),
            load_date=datetime.fromisoformat(timestamp),
        )
        if data["batch_id"] != context.batch_id or timestamp != context.load_date_utc:
            raise ContractError("Persisted batch identity or UTC timestamp is inconsistent")
        return context
