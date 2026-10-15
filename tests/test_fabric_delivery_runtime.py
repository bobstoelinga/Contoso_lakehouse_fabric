from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from fabric_etl.contracts import ContractError, canonical_json, fingerprint
from fabric_etl.delivery import Manifest
from fabric_etl_phase5.delivery import (
    InterruptedDelivery, _document, _file_identity, ingest_registered_delivery, register_delivery, verify_onelake_files,
)


def document():
    return {
        "contract_version": "1.0", "project_id": "contoso", "source_system_id": "PHASE4_PROOF",
        "delivery_id": "proof", "delivery_sequence": 1, "schema_contract_version": "release",
        "status": "CLOSED", "objects": [{
            "source_object_id": "EXIT.JSON", "files": [{"path": "a.json", "size_bytes": 3, "sha256": "a" * 64}],
            "complete_snapshot": False, "empty_snapshot": False,
        }],
    }


def manifest(value):
    return Manifest.from_dict(value, project_id="contoso", source_system_id="PHASE4_PROOF",
                              required_objects={"EXIT.JSON"}, schema_contract_version="release")


def test_dataclass_manifest_roundtrip_keeps_full_file_register():
    value = document()
    assert _document(manifest(value)) == value


@pytest.mark.parametrize("change", ["sequence", "fingerprint", "scope", "duplicate"])
def test_delivery_registration_rejects_conflicting_persisted_contract(monkeypatch, change):
    value = document()
    row = {
        "delivery_id": "proof", "delivery_sequence": 1,
        "manifest_fingerprint": fingerprint(value), "scope_json": canonical_json(value).decode(),
    }
    if change == "sequence":
        row["delivery_sequence"] = 2
    elif change == "fingerprint":
        row["manifest_fingerprint"] = "b" * 64
    elif change == "scope":
        row["scope_json"] = "{}"
    monkeypatch.setattr("fabric_etl_phase5.delivery.rows", lambda *args: [row, row] if change == "duplicate" else [row])
    control = SimpleNamespace(project="contoso", spark=None, table=lambda name: name, assert_owner=lambda owner: None)
    with pytest.raises(ContractError, match="conflicts"):
        register_delivery(control, "owner", value, manifest(value), now=datetime.now(timezone.utc))


def test_registration_retry_reads_same_record_without_mutation(monkeypatch):
    value = document()
    row = {
        "delivery_id": "proof", "delivery_sequence": 1, "status": "REGISTERED",
        "manifest_fingerprint": fingerprint(value), "scope_json": canonical_json(value).decode(),
    }
    monkeypatch.setattr("fabric_etl_phase5.delivery.rows", lambda *args: [row])
    control = SimpleNamespace(project="contoso", spark=None, table=lambda name: name, assert_owner=lambda owner: None)
    assert register_delivery(control, "owner", value, manifest(value), now=datetime.now(timezone.utc)) == row


@pytest.mark.parametrize("change", ["missing", "extra", "bytes", "size", "manifest", "no_manifest"])
def test_onelake_gate_rejects_actual_inventory_manifest_and_bytes(monkeypatch, change):
    value = document()
    entries = [
        SimpleNamespace(name="_manifest.json", path="root/_manifest.json", isDir=False, size=300),
        SimpleNamespace(name="a.json", path="root/a.json", isDir=False, size=3),
    ]
    if change == "missing":
        entries.pop()
    elif change == "extra":
        entries.append(SimpleNamespace(name="extra.json", path="extra", isDir=False, size=1))
    elif change == "no_manifest":
        entries.pop(0)
    fs = SimpleNamespace(ls=lambda path: entries, head=lambda *args, **kwargs:
                         "{}" if change == "manifest" else canonical_json(value).decode())
    result = {"length": 4 if change == "size" else 3, "sha256": "b" * 64 if change == "bytes" else "a" * 64}
    monkeypatch.setattr("fabric_etl_phase5.delivery._file_identity", lambda *args: result)
    with pytest.raises(ContractError):
        verify_onelake_files(None, fs, "root", manifest(value))


def test_empty_snapshot_requires_physical_closed_manifest_but_no_files():
    value = document()
    value["objects"][0].update(files=[], complete_snapshot=True, empty_snapshot=True)
    fs = SimpleNamespace(
        ls=lambda path: [SimpleNamespace(name="_manifest.json", path="manifest", size=300, isDir=False)],
        head=lambda *args, **kwargs: canonical_json(value).decode(),
    )
    assert verify_onelake_files(None, fs, "root", manifest(value)) == []


@pytest.mark.parametrize("fail", [False, True])
def test_streamed_hash_counts_consumed_bytes_and_closes_streams(monkeypatch, fail):
    import hashlib

    closed = []
    payload = b"PAR1\x00\xffPAR1"
    digest = hashlib.sha256()
    sink = SimpleNamespace(close=lambda: closed.append("sink"))

    def transfer_to(target):
        assert target is sink
        if fail:
            raise OSError("stream failure")
        digest.update(payload)
        return len(payload)

    stream = SimpleNamespace(transferTo=transfer_to, close=lambda: closed.append("stream"))
    filesystem = SimpleNamespace(open=lambda path: "input")
    path = SimpleNamespace(getFileSystem=lambda configuration: filesystem)
    java_digest = SimpleNamespace(digest=lambda: [value if value < 128 else value - 256 for value in digest.digest()])
    jvm = SimpleNamespace(
        org=SimpleNamespace(apache=SimpleNamespace(hadoop=SimpleNamespace(fs=SimpleNamespace(Path=lambda value: path)))),
        java=SimpleNamespace(
            security=SimpleNamespace(
                MessageDigest=SimpleNamespace(getInstance=lambda name: java_digest),
                DigestInputStream=lambda raw, hasher: stream,
            ),
            io=SimpleNamespace(OutputStream=SimpleNamespace(nullOutputStream=lambda: sink)),
        ),
    )
    spark = SimpleNamespace(_jvm=jvm, _jsc=SimpleNamespace(hadoopConfiguration=lambda: "configuration"))
    if fail:
        with pytest.raises(OSError, match="stream failure"):
            _file_identity(spark, "file")
    else:
        assert _file_identity(spark, "file") == {"length": len(payload), "sha256": hashlib.sha256(payload).hexdigest()}
    assert closed == ["stream", "sink"]


def runtime_case(monkeypatch, *, existing_work=None, empty=False):
    from pyspark.sql import functions as F
    monkeypatch.setattr(F, "col", lambda name: SimpleNamespace(__eq__=lambda other: None))
    value = document()
    if empty:
        value["objects"][0].update(files=[], complete_snapshot=True, empty_snapshot=True)
    calls = []
    record_count = 0 if empty else 3
    frame = SimpleNamespace(
        cache=lambda: frame, count=lambda: record_count, where=lambda condition: frame,
        unpersist=lambda: calls.append(("unpersist",)),
    )
    control = SimpleNamespace(
        project="contoso", spark=None, table=lambda name: name,
        environment=SimpleNamespace(spark_table=lambda *args: "target"),
        batch_context=lambda *args, **kwargs: SimpleNamespace(batch_id="batch", load_date_utc="time"),
    )
    module = "fabric_etl_phase5.delivery."
    monkeypatch.setattr(module + "register_delivery", lambda *args, **kwargs: {"status": "REGISTERED"})
    monkeypatch.setattr(module + "verify_onelake_files", lambda *args: [])
    monkeypatch.setattr(module + "_delivery_status", lambda *args: calls.append(("delivery", args[-1])))
    monkeypatch.setattr(module + "_work", lambda *args, **kwargs: calls.append(("work", args[6], args[5], args[8])))
    monkeypatch.setattr(module + "_audit", lambda *args, **kwargs: calls.append(("audit", args[5], args[6])) or "audit")
    monkeypatch.setattr(module + "read_manifest_records", lambda *args, **kwargs: frame)
    monkeypatch.setattr(module + "write_bronze_delivery", lambda *args, **kwargs: {
        "target_table": "target", "record_count": record_count, "output_fingerprint": "digest",
        "inserted_count": record_count, "retry_count": 0,
    })
    monkeypatch.setattr(module + "commit_layer", lambda *args, **kwargs: calls.append(("commit",)) or {
        "commit_id": "commit", "status": "COMMITTED",
    })

    def rows(spark, query):
        if "work_item" in query:
            return [existing_work] if existing_work else []
        return [{"delivery_id": "proof", "delivery_sequence": 1, "status": "READY"}]

    monkeypatch.setattr(module + "rows", rows)
    args = dict(
        fs=None, root="root", document=value, required_objects={"EXIT.JSON"},
        source_formats={"EXIT.JSON": "JSON"}, schemas={"EXIT.JSON": SimpleNamespace(json=lambda: "schema")},
        targets={"EXIT.JSON": "phase5_exit_json"}, now=datetime.now(timezone.utc), proof_only=True,
    )
    return control, args, calls


@pytest.mark.parametrize("stage", ["raw", "bronze", "commit", "status"])
def test_explicit_interruption_preserves_boundary_without_success_fallback(monkeypatch, stage):
    control, args, calls = runtime_case(monkeypatch)

    def stop(name):
        if name == stage:
            raise InterruptedDelivery(stage)

    with pytest.raises(InterruptedDelivery):
        ingest_registered_delivery(control, "owner", **args, boundary=stop)
    assert calls[-1] == ("unpersist",)
    assert not any(call[:2] == ("audit", "SUCCEEDED") for call in calls)
    assert not any(call[:2] == ("work", "FAILED") for call in calls)
    assert (("commit",) in calls) == (stage in {"commit", "status"})
    assert any(call[:2] == ("work", "SUCCEEDED") for call in calls) == (stage == "status")


def test_processing_failure_is_persisted_audited_and_rethrown(monkeypatch):
    control, args, calls = runtime_case(monkeypatch)

    def fail(name):
        if name == "raw":
            raise ContractError("invalid source")

    with pytest.raises(ContractError, match="invalid source"):
        ingest_registered_delivery(control, "owner", **args, boundary=fail)
    assert ("work", "FAILED", 1, "ContractError") in calls
    assert ("audit", "FAILED", "ContractError") in calls
    assert ("delivery", "FAILED") in calls
    assert ("commit",) not in calls
    assert calls[-1] == ("unpersist",)


def test_empty_delivery_has_explicit_context_zero_output_and_commit(monkeypatch):
    control, args, calls = runtime_case(monkeypatch, empty=True)
    result = ingest_registered_delivery(control, "owner", **args)
    assert result["batch_id"] == "batch"
    assert result["outputs"]["EXIT.JSON"]["record_count"] == 0
    assert result["commit"]["commit_id"] == "commit"
    assert ("audit", "SUCCEEDED", None) in calls
    assert calls.index(("commit",)) < calls.index(("delivery", "SUCCEEDED"))
