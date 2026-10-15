"""Manifest validation and registered-delivery chronology."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

from fabric_etl.contracts import ContractError, fields, identifier, required_text


@dataclass(frozen=True)
class FileEntry:
    path: str
    size_bytes: int
    sha256: str


@dataclass(frozen=True)
class ObjectEntry:
    source_object_id: str
    files: tuple[FileEntry, ...]
    complete_snapshot: bool
    empty_snapshot: bool


@dataclass(frozen=True)
class Manifest:
    project_id: str
    source_system_id: str
    delivery_id: str
    delivery_sequence: int
    schema_contract_version: str
    objects: tuple[ObjectEntry, ...]

    @classmethod
    def from_dict(
        cls, value: object, *, project_id: str, source_system_id: str,
        required_objects: set[str], schema_contract_version: str,
    ) -> Manifest:
        data = fields(value, {
            "contract_version", "project_id", "source_system_id", "delivery_id",
            "delivery_sequence", "schema_contract_version", "status", "objects",
        }, "Manifest")
        if data["contract_version"] != "1.0" or data["status"] != "CLOSED":
            raise ContractError("Only CLOSED manifest contract 1.0 is supported")
        if data["project_id"] != project_id or data["source_system_id"] != source_system_id:
            raise ContractError("Manifest belongs to a different project or source")
        if data["schema_contract_version"] != schema_contract_version:
            raise ContractError("Manifest schema contract version mismatch")
        sequence = data["delivery_sequence"]
        if type(sequence) is not int or sequence < 1:
            raise ContractError("delivery_sequence must be a positive integer")
        raw_objects = data["objects"]
        if not isinstance(raw_objects, list) or not raw_objects or not required_objects:
            raise ContractError("Manifest and required object scope must be non-empty")
        entries: list[ObjectEntry] = []
        seen_objects: set[str] = set()
        seen_files: set[str] = set()
        for raw_object in raw_objects:
            obj = fields(raw_object, {
                "source_object_id", "files", "complete_snapshot", "empty_snapshot",
            }, "Manifest object")
            object_id = required_text(obj["source_object_id"], "source_object_id")
            if object_id in seen_objects:
                raise ContractError(f"Duplicate manifest object: {object_id}")
            seen_objects.add(object_id)
            complete, empty = obj["complete_snapshot"], obj["empty_snapshot"]
            if type(complete) is not bool or type(empty) is not bool:
                raise ContractError("Snapshot declarations must be booleans")
            raw_files = obj["files"]
            if not isinstance(raw_files, list):
                raise ContractError("files must be a list")
            if (empty and (not complete or raw_files)) or (not empty and not raw_files):
                raise ContractError("Missing files are valid only for an explicit complete empty snapshot")
            files: list[FileEntry] = []
            for raw_file in raw_files:
                file = fields(raw_file, {"path", "size_bytes", "sha256"}, "Manifest file")
                path = required_text(file["path"], "file path")
                parts = path.split("/")
                if (
                    PurePosixPath(path).is_absolute()
                    or path == "_manifest.json"
                    or any(part in {"", ".", ".."} for part in parts)
                    or any(not re.fullmatch(r"[A-Za-z0-9_.-]+", part) for part in parts)
                    or path.casefold() in seen_files
                ):
                    raise ContractError(f"Unsafe or duplicate raw file path: {path}")
                seen_files.add(path.casefold())
                size = file["size_bytes"]
                digest = file["sha256"]
                if type(size) is not int or size < 0:
                    raise ContractError("File size must be a non-negative integer")
                if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest):
                    raise ContractError("File fingerprint must be lowercase SHA-256")
                files.append(FileEntry(path, size, digest))
            entries.append(ObjectEntry(object_id, tuple(files), complete, empty))
        if seen_objects != required_objects:
            raise ContractError("Manifest object scope does not match the required source scope")
        return cls(
            identifier(project_id), identifier(source_system_id),
            required_text(data["delivery_id"], "delivery_id"), sequence,
            schema_contract_version, tuple(entries),
        )

    def verify_local_files(self, root: Path) -> None:
        root = root.resolve(strict=True)
        declared = {
            file.path.casefold()
            for obj in self.objects
            for file in obj.files
        }
        actual = {
            path.relative_to(root).as_posix().casefold()
            for path in root.rglob("*")
            if path.is_file() and path.relative_to(root).as_posix() != "_manifest.json"
        }
        if actual != declared:
            missing = sorted(declared - actual)
            unexpected = sorted(actual - declared)
            raise ContractError(
                f"Delivery file inventory differs from manifest "
                f"(missing={missing}, unexpected={unexpected})"
            )
        for obj in self.objects:
            for file in obj.files:
                try:
                    path = root.joinpath(*file.path.split("/")).resolve(strict=True)
                except OSError as exc:
                    raise ContractError(f"Manifest file is missing: {file.path}") from exc
                if not path.is_relative_to(root) or not path.is_file():
                    raise ContractError(f"Raw file escapes delivery root or is not a file: {file.path}")
                digest = hashlib.sha256()
                size = 0
                with path.open("rb") as stream:
                    for block in iter(lambda: stream.read(1024 * 1024), b""):
                        size += len(block)
                        digest.update(block)
                if size != file.size_bytes or digest.hexdigest() != file.sha256:
                    raise ContractError(f"Raw file content differs from manifest: {file.path}")


@dataclass(frozen=True)
class RegisteredDelivery:
    delivery_id: str
    sequence: int
    status: str


def next_delivery(deliveries: list[RegisteredDelivery]) -> RegisteredDelivery | None:
    """Caller supplies one project/source scope, including open deliveries."""
    statuses = {
        "REGISTERED", "WAITING", "READY", "PROCESSING", "SUCCEEDED",
        "INVALID", "FAILED", "QUARANTINED", "SUPERSEDED",
    }
    seen_ids: set[str] = set()
    seen_sequences: set[int] = set()
    for delivery in deliveries:
        required_text(delivery.delivery_id, "delivery_id")
        if (
            type(delivery.sequence) is not int or delivery.sequence < 1
            or delivery.status not in statuses
            or delivery.delivery_id in seen_ids or delivery.sequence in seen_sequences
        ):
            raise ContractError("Invalid or duplicate registered delivery")
        seen_ids.add(delivery.delivery_id)
        seen_sequences.add(delivery.sequence)
    pending = sorted(
        (d for d in deliveries if d.status not in {"SUCCEEDED", "SUPERSEDED"}),
        key=lambda d: d.sequence,
    )
    if not pending or pending[0].status != "READY":
        return None
    return pending[0]


def validate_ready_delivery_manifest(
    value: object, *, root: Path, project_id: str, source_system_id: str,
    required_objects: set[str], schema_contract_version: str,
    registered_deliveries: list[RegisteredDelivery],
) -> Manifest:
    """Validate a closed delivery and its bytes before releasing it to ingestion."""
    manifest = Manifest.from_dict(
        value,
        project_id=project_id,
        source_system_id=source_system_id,
        required_objects=required_objects,
        schema_contract_version=schema_contract_version,
    )
    selected = next_delivery(registered_deliveries)
    if selected is None:
        raise ContractError("No chronologically processable READY delivery")
    if (
        selected.delivery_id != manifest.delivery_id
        or selected.sequence != manifest.delivery_sequence
    ):
        raise ContractError("Manifest is not the chronologically next READY delivery")
    manifest.verify_local_files(root)
    return manifest
