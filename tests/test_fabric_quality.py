import pytest
from datetime import datetime, timezone
from types import SimpleNamespace

from fabric_etl.contracts import ContractError, canonical_json, fingerprint
from fabric_etl_phase5.quality import QualityBlocked, Rule, commit_layer, threshold_results


def rule(threshold=25.0, action="FAIL_BATCH"):
    return Rule("key_present", "customer_id IS NOT NULL", "ERROR", "MISSING_KEY", threshold, action)


def test_threshold_is_strictly_greater_than_configured_and_uses_filtered_population():
    result = threshold_results([rule()], population=4, violations={"key_present": 1})
    assert result[0]["violation_percent"] == 25
    assert result[0]["breached"] is False
    with pytest.raises(QualityBlocked, match="key_present:FAIL_BATCH"):
        threshold_results([rule(24.99)], population=4, violations={"key_present": 1})


def test_empty_population_has_zero_violations():
    assert threshold_results([rule(0)], population=0, violations={"key_present": 0})[0]["breached"] is False


@pytest.mark.parametrize("action", ["FAIL_BATCH", "QUARANTINE_BATCH"])
def test_blocking_actions_never_return_success(action):
    with pytest.raises(QualityBlocked, match=action):
        threshold_results([rule(0, action)], population=1, violations={"key_present": 1})


def test_continue_records_a_breach_without_hiding_it():
    assert threshold_results([rule(0, "CONTINUE")], population=1, violations={"key_present": 1})[0]["breached"]


@pytest.mark.parametrize("threshold", [-1, 101, True, float("nan"), float("inf"), "25"])
def test_invalid_thresholds_are_rejected(threshold):
    with pytest.raises(ContractError):
        rule(threshold)


@pytest.mark.parametrize("population,counts", [
    (-1, {"key_present": 0}), (True, {"key_present": 0}),
    (1, {"key_present": 2}), (1, {"key_present": -1}),
    (1, {"key_present": True}), (1, {}), (1, {"unknown": 0}),
])
def test_invalid_metrics_fail_closed(population, counts):
    with pytest.raises(ContractError):
        threshold_results([rule()], population=population, violations=counts)


def test_duplicate_rule_identity_is_rejected():
    with pytest.raises(ContractError, match="Duplicate"):
        threshold_results([rule(), rule()], population=4, violations={"key_present": 1})


@pytest.mark.parametrize("identity", ["DQ-CUST-001", "DQ.CUST.001", "key_present"])
def test_metadata_rule_identity_is_not_a_sql_column_name(identity):
    value = Rule(identity, "customer_id IS NOT NULL", "ERROR", "MISSING_KEY", 100, "CONTINUE")
    assert threshold_results([value], population=1, violations={identity: 0})[0]["rule_id"] == identity


@pytest.mark.parametrize("identity", ["", "a`b", "a b", "1rule", None])
def test_invalid_metadata_rule_identity_fails_closed(identity):
    with pytest.raises(ContractError, match="rule identity"):
        Rule(identity, "TRUE", "ERROR", "BAD", 100, "CONTINUE")


def test_existing_commit_is_verified_without_mutation_and_conflicts_fail(monkeypatch):
    outputs = {"PASSED": {"record_count": 3, "output_fingerprint": "deterministic"}}
    commit_id = fingerprint(["contoso", "batch", "QUALITY", "PROBE.CUSTOMERS", "input", outputs])
    existing = {
        "commit_id": commit_id, "input_fingerprint": "input",
        "outputs_json": canonical_json(outputs).decode(), "status": "COMMITTED",
    }
    monkeypatch.setattr("fabric_etl.control.rows", lambda spark, query: [existing])
    claims = []
    control = SimpleNamespace(
        project="contoso", spark=None, table=lambda name: "commit_manifest",
        assert_owner=lambda owner: claims.append(owner),
    )
    args = dict(batch_id="batch", layer="QUALITY", entity="PROBE.CUSTOMERS",
                input_fingerprint="input", outputs=outputs, now=datetime.now(timezone.utc))
    assert commit_layer(control, "owner", **args) == {"commit_id": commit_id, "status": "VERIFIED"}
    assert claims == ["owner"]
    existing["outputs_json"] = "{}"
    with pytest.raises(ContractError, match="conflicts"):
        commit_layer(control, "owner", **args)
