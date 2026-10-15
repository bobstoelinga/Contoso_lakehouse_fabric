from datetime import datetime, timedelta, timezone

import pytest

from fabric_etl.contracts import ContractError
from fabric_etl_phase6.pit import pit_spec, project_pit, write_pit
from test_fabric_sales_vault import project


def test_pit_spec_requires_explicit_registered_customer_event_parents():
    assert pit_spec(project())["keys"] == ["hk_customer"]


@pytest.mark.parametrize("change", ["missing", "duplicate", "type", "keys", "parents", "temporal_policy"])
def test_pit_cannot_fall_back_or_accept_an_incomplete_parent_contract(change):
    release = project()
    entity = next(entity for entity in release.document["business_entities"] if entity["id"] == "PIT_CUSTOMER")
    if change == "missing":
        release.document["business_entities"].remove(entity)
    elif change == "duplicate":
        release.document["business_entities"].append(entity)
    else:
        entity[change] = [] if change in {"keys", "parents"} else "wrong"
    with pytest.raises(ContractError):
        pit_spec(release)


def test_pit_writer_does_not_read_or_write_without_explicit_proof_scope():
    with pytest.raises(ContractError, match="explicit isolated"):
        write_pit(None, None, project=project(), delivery={}, now=datetime.now(timezone.utc))


@pytest.mark.parametrize("as_of", [
    datetime(2026, 10, 12), datetime(2026, 10, 12, tzinfo=timezone(timedelta(hours=2))),
])
def test_pit_cutoff_cannot_guess_timezone_or_accept_non_utc(as_of):
    with pytest.raises(ContractError, match="UTC"):
        project_pit(None, None, None, project_id="contoso", batch_id="batch", as_of=as_of)
