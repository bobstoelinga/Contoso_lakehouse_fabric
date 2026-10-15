import pytest
from datetime import datetime, timezone

from fabric_etl.contracts import ContractError
from fabric_etl_phase6.proof import expected_event_count, expected_gold_rows, expected_vault_rows

from test_fabric_sales_vault import project
from fabric_etl_phase6.vault import vault_spec


def test_independent_expected_business_payload_covers_every_declared_vault_and_bv_column():
    release = project()
    for number in (1, 2, 3):
        expected = expected_vault_rows(number)
        assert len(expected) == 18
        for entity_id, values in expected.items():
            spec = vault_spec(release, entity_id, business=entity_id.endswith("_BV"))
            assert set(values) == {column["name"] for column in spec["schema_columns"]} | {spec["key_column"]}


def test_expected_changes_and_deletes_do_not_create_spurious_hubs_links_or_bv_events():
    expected = expected_vault_rows(1)
    assert sum(expected_event_count(entity, 1) for entity in expected) == 18
    assert sum(expected_event_count(entity, 2) for entity in expected) == 2
    assert sum(expected_event_count(entity, 3) for entity in expected) == 3
    assert expected_vault_rows(1)["SAT_CUSTOMER_BV"] == expected_vault_rows(3)["SAT_CUSTOMER_BV"]
    assert expected_vault_rows(1)["SAT_ORDER_LINE"] == expected_vault_rows(3)["SAT_ORDER_LINE"]
    assert expected_vault_rows(3)["SAT_CUSTOMER"]["is_deleted"] is True


@pytest.mark.parametrize("number", [0, 4, True, "1"])
def test_unregistered_expected_scenarios_fail_closed(number):
    with pytest.raises(ContractError):
        expected_vault_rows(number)


def test_unregistered_entity_event_expectations_fail_closed():
    with pytest.raises(ContractError):
        expected_event_count("PIT_CUSTOMER", 1)


def test_independent_gold_rows_cover_all_eleven_declared_entity_shapes_and_full_calendar():
    release = project()
    times = [datetime(2026, 10, 12, 6, 0, number, tzinfo=timezone.utc) for number in (1, 2, 3)]
    expected = expected_gold_rows(3, times)
    entities = [entity for entity in release.document["gold_entities"] if entity["source_system_id"] == "SALES"]
    assert set(expected) == {entity["id"] for entity in entities}
    for entity in entities:
        columns = next(schema["columns"] for schema in release.document["schemas"] if schema["id"] == entity["schema_id"])
        for row in expected[entity["id"]]:
            assert set(row) == {column["name"] for column in columns if not column["name"].startswith("_")}
    assert len(expected["GC_DIM_DATE"]) == 4018
    assert expected["GC_DIM_DATE"][0]["date_key"] == 20200101
    assert expected["GC_DIM_DATE"][-1]["date_key"] == 20301231
    assert len(expected["GH_DIM_CUSTOMER"]) == 3
    assert len(expected["GH_DIM_EMPLOYEE"]) == 2
    assert expected["GC_DIM_CUSTOMER"] == []
    assert expected["GC_FCT_SALES"][0]["net_amount"] == 20
