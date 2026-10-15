from pathlib import Path
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from fabric_etl.contracts import ContractError
from fabric_etl.project import ProjectRelease
from fabric_etl_phase5.mapping import quality_spec, reject_invalid_casts, write_metadata_quality
from fabric_etl_phase5.ingestion import RAW_RECORD_COLUMNS


ROOT = Path(__file__).resolve().parents[1]
PROJECT = ProjectRelease.load(ROOT / "projects" / "contoso" / "release.json")


@pytest.mark.parametrize("expression,expected,count", [
    ("cast(amount as decimal(18,4))", "try_cast(amount AS DECIMAL(18,4))", 1),
    ("coalesce(cast(flag as boolean), false)", "coalesce(try_cast(flag AS BOOLEAN), false)", 1),
    ("trim(cast(key as string))", "trim(try_cast(key AS STRING))", 1),
    ("cast(cast(value as int) as bigint)", "try_cast(try_cast(value AS INT) AS LONG)", 2),
    ("concat('cast(x as int)', cast(`odd` as date))", "concat('cast(x as int)', try_cast(`odd` AS DATE))", 1),
    ("try_cast(x as decimal(18,8))", "try_cast(x as decimal(18,8))", 0),
])
def test_cast_rewrite_preserves_expression_and_each_inner_validity_guard(expression, expected, count):
    actual, guards = reject_invalid_casts(expression)
    assert actual == expected
    assert len(guards) == count
    assert all("IS NULL OR try_cast(" in guard for guard in guards)


@pytest.mark.parametrize("expression", ["cast(x)", "cast(x as int", "'unclosed", "x; DELETE FROM t", "x -- comment"])
def test_invalid_mapping_expressions_fail_closed(expression):
    with pytest.raises(ContractError):
        reject_invalid_casts(expression)


def test_all_actual_quality_mappings_and_all_33_rules_compile_without_legacy_bindings():
    views = {source["id"]: f"ref_{index}" for index, source in enumerate(PROJECT.document["source_objects"])}
    compiled = [quality_spec(PROJECT, source["id"], reference_views=views)
                for source in PROJECT.document["source_objects"]]
    assert len(compiled) == 9
    assert sum(len(spec["metadata_rule_ids"]) for spec in compiled) == 33
    assert sum(len(spec["projections"]) for spec in compiled) == sum(
        mapping["entity_id"].startswith("source_objects:") for mapping in PROJECT.document["mappings"])
    for spec in compiled:
        assert all("{quality_catalog}" not in rule.expression for rule in spec["rules"])
        assert spec["release_id"] == PROJECT.release_id
        assert all(item["guard_column"].startswith("_mapping_valid_") for item in spec["projections"])


def test_ecb_filter_is_raw_preprojection_keep_semantics_not_inverted():
    spec = quality_spec(PROJECT, "ECB.EXCHANGE_RATE", reference_views={})
    assert spec["excluded_expression"] == "NOT ((try_cast(OBS_VALUE AS decimal(18,8)) IS NOT NULL))"
    assert spec["projections"][0]["column"] == "currency_code"
    assert "CURRENCY" in spec["projections"][0]["expression"]


def test_referential_rules_require_closed_batch_scoped_views():
    with pytest.raises(ContractError, match="Batch-scoped"):
        quality_spec(PROJECT, "SALES.ORDERS", reference_views={})
    spec = quality_spec(PROJECT, "SALES.ORDERS", reference_views={
        "SALES.CUSTOMERS": "customers", "SALES.PRODUCTS": "products", "SALES.EMPLOYEES": "employees",
    })
    assert spec["reference_sources"] == ["SALES.CUSTOMERS", "SALES.EMPLOYEES", "SALES.PRODUCTS"]
    assert any("FROM `customers`" in rule.expression for rule in spec["rules"])


def test_blocking_metadata_actions_and_generated_cast_reasons_are_explicit():
    spec = quality_spec(PROJECT, "SALES.CUSTOMERS", reference_views={})
    actual = {rule.rule_id: rule for rule in spec["rules"]}
    assert actual["DQ-CUST-001"].on_threshold_breach == "FAIL_BATCH"
    assert actual["DQ-CUST-003"].on_threshold_breach == "CONTINUE"
    boolean = next(item for item in spec["projections"] if item["column"] == "is_deleted")
    assert "try_cast(is_deleted AS BOOLEAN) IS NOT NULL" in boolean["guard_expression"]
    assert boolean["expression"].startswith("try_cast((coalesce(")


def test_rules_bind_presence_and_time_to_the_declared_payload_and_persisted_batch():
    orders = quality_spec(PROJECT, "SALES.ORDERS", reference_views={
        "SALES.CUSTOMERS": "customers", "SALES.PRODUCTS": "products", "SALES.EMPLOYEES": "employees",
    })
    dated = next(rule for rule in orders["rules"] if rule.rule_id == "DQ-ORD-007")
    assert "current_date" not in dated.expression
    assert "cast(load_date_utc AS DATE)" in dated.expression
    nager = quality_spec(PROJECT, "NAGER.HOLIDAYS_NL", reference_views={})
    rule = next(rule for rule in nager["rules"] if rule.rule_id == "DQ-NAG-002")
    assert "size(from_json(holiday_types, 'array<string>'))" in rule.expression


def writer_case(monkeypatch):
    from pyspark.sql import functions as F

    class Expression:
        def __eq__(self, other):
            return self

        def __and__(self, other):
            return self

    monkeypatch.setattr(F, "lit", lambda value: Expression())
    monkeypatch.setattr(F, "col", lambda name: Expression())
    stamp = datetime(2026, 10, 11, tzinfo=timezone.utc)
    context = dict(
        project_id="contoso", source_system_id="PHASE4_PROOF", delivery_id="proof",
        batch_id="batch", release_id=PROJECT.release_id, source_object_id="SALES.CUSTOMERS",
        load_date_utc=stamp.isoformat(timespec="microseconds"), delivery_sequence=1,
    )

    class Row(dict):
        def asDict(self):
            return dict(self)

    class Frame:
        columns = list(RAW_RECORD_COLUMNS)

        def select(self, *columns):
            return self

        def distinct(self):
            return self

        def collect(self):
            return [Row(context)]

        def where(self, expression):
            return self

        def exceptAll(self, other):
            return self

        def limit(self, count):
            return self

        def count(self):
            return 0

        def cache(self):
            return self

        def unpersist(self):
            calls.append("unpersist")

    frame = Frame()
    calls = []
    batch = {**context, "release_id": PROJECT.release_id, "load_date": stamp}
    bronze = {
        "status": "COMMITTED", "layer": "BRONZE", "batch_id": "batch",
        "entity_id": "PHASE4_PROOF.proof",
        "outputs_json": '{"target_table":"`processing`.`bronze`.`customers`",'
                        '"record_count":2,"output_fingerprint":"bronze-digest"}',
    }
    monkeypatch.setattr("fabric_etl.control.rows", lambda spark, query: [batch] if "batch_context" in query else [bronze])
    monkeypatch.setattr("fabric_etl_phase5.ingestion._output_fingerprint", lambda stored: (2, "bronze-digest"))
    accounting = {"bronze_count": 2, "counts": {"PASSED": 1, "REJECTED": 1, "FILTERED": 0}}
    monkeypatch.setattr("fabric_etl_phase5.mapping.evaluate_metadata_quality", lambda *args, **kwargs: (frame, accounting))

    def output(control, owner, outcomes, **kwargs):
        calls.append(kwargs)
        return {
            "target_table": kwargs["schema"] + "." + kwargs["table"], "record_count": 1,
            "output_fingerprint": "digest", "inserted_count": 1, "retry_count": 0,
        }

    monkeypatch.setattr("fabric_etl_phase5.quality.write_immutable_output", output)

    def commit(control, owner, **kwargs):
        calls.append(kwargs)
        return {"commit_id": "quality-commit", "status": "COMMITTED"}

    monkeypatch.setattr("fabric_etl_phase5.quality.commit_layer", commit)
    control = SimpleNamespace(
        project="contoso", spark=SimpleNamespace(table=lambda name: frame), table=lambda name: name,
        environment=SimpleNamespace(spark_table=lambda role, schema, table: f"`processing`.`{schema}`.`{table}`"),
        assert_owner=lambda owner: None,
    )
    args = dict(source_schema=SimpleNamespace(json=lambda: "source-schema"), project=PROJECT,
                source_id="SALES.CUSTOMERS", bronze_commit_id="bronze-commit",
                table="phase5_metadata_customers", now=stamp, proof_only=True)
    return control, frame, args, context, batch, bronze, calls


def test_metadata_writer_reconciles_bronze_writes_all_statuses_and_commits_stable_outputs(monkeypatch):
    control, frame, args, _, _, _, calls = writer_case(monkeypatch)
    result = write_metadata_quality(control, "owner", frame, **args)
    assert result["quality_commit"]["commit_id"] == "quality-commit"
    assert [call["schema"] for call in calls[:3]] == ["quality", "reject", "quality"]
    assert calls[2]["table"].endswith("_filtered")
    assert calls[0]["scope"]["release_id"] == PROJECT.release_id
    commit = calls[3]
    assert commit["layer"] == "QUALITY"
    assert commit["entity"] == "SALES.CUSTOMERS"
    assert len(commit["input_fingerprint"]) == 64
    assert "inserted_count" not in commit["outputs"]["targets"]["PASSED"]
    assert calls[-1] == "unpersist"


@pytest.mark.parametrize("change", [
    "null", "release", "source", "batch", "timestamp", "commit", "layer", "entity", "fingerprint", "namespace",
])
def test_metadata_writer_fails_before_outputs_for_untrusted_inputs(monkeypatch, change):
    control, frame, args, context, batch, bronze, calls = writer_case(monkeypatch)
    if change == "null":
        context["delivery_id"] = None
    elif change == "release":
        context["release_id"] = "other"
    elif change == "source":
        context["source_object_id"] = "SALES.ORDERS"
    elif change == "batch":
        batch["delivery_sequence"] = 2
    elif change == "timestamp":
        batch["load_date"] = datetime(2025, 1, 1)
    elif change == "commit":
        bronze["status"] = "PENDING"
    elif change == "layer":
        bronze["layer"] = "QUALITY"
    elif change == "entity":
        bronze["entity_id"] = "other"
    elif change == "fingerprint":
        bronze["outputs_json"] = bronze["outputs_json"].replace("bronze-digest", "changed")
    else:
        bronze["outputs_json"] = bronze["outputs_json"].replace("`processing`", "`gold`")
    with pytest.raises(ContractError):
        write_metadata_quality(control, "owner", frame, **args)
    assert calls == []


def test_metadata_writer_does_not_accept_uncommitted_references_or_unisolated_proof_tables(monkeypatch):
    control, frame, args, _, _, _, calls = writer_case(monkeypatch)
    args["source_id"] = "SALES.ORDERS"
    with pytest.raises(ContractError, match="Batch-scoped"):
        write_metadata_quality(control, "owner", frame, **args)
    args["source_id"] = "SALES.CUSTOMERS"
    args["table"] = "business_customers"
    with pytest.raises(ContractError, match="isolated"):
        write_metadata_quality(control, "owner", frame, **args)
    assert calls == []


def test_metadata_writer_rejects_empty_snapshot_without_inventing_context(monkeypatch):
    control, frame, args, _, _, _, calls = writer_case(monkeypatch)
    monkeypatch.setattr(frame, "collect", lambda: [])
    with pytest.raises(ContractError, match="nonempty"):
        write_metadata_quality(control, "owner", frame, **args)
    assert calls == []


def test_metadata_writer_rejects_changed_durable_bronze_before_outputs(monkeypatch):
    control, frame, args, _, _, _, calls = writer_case(monkeypatch)
    mismatch = SimpleNamespace(limit=lambda count: SimpleNamespace(count=lambda: 1))
    monkeypatch.setattr(frame, "exceptAll", lambda other: mismatch)
    with pytest.raises(ContractError, match="readback"):
        write_metadata_quality(control, "owner", frame, **args)
    assert calls == []


def test_metadata_writer_business_mode_requires_active_approved_release(monkeypatch):
    control, frame, args, context, batch, _, calls = writer_case(monkeypatch)
    context["source_system_id"] = batch["source_system_id"] = "SALES"
    args["proof_only"] = False
    with pytest.raises(ContractError, match="active approved"):
        write_metadata_quality(control, "owner", frame, **args)
    assert calls == []


def test_metadata_writer_unpersists_and_never_commits_when_output_fails(monkeypatch):
    control, frame, args, _, _, _, calls = writer_case(monkeypatch)

    def fail(*args, **kwargs):
        raise ContractError("Output readback conflict")

    monkeypatch.setattr("fabric_etl_phase5.quality.write_immutable_output", fail)
    with pytest.raises(ContractError, match="conflict"):
        write_metadata_quality(control, "owner", frame, **args)
    assert calls == ["unpersist"]
