import os
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from fabric_etl.canonical import scalar
from fabric_etl.contracts import ContractError, hash_key
from fabric_etl_phase5.ingestion import spark_schema
from fabric_etl_phase6.pit import project_pit
from fabric_etl_phase6.gold import gold_columns, project_gold, sales_gold_entities
from fabric_etl_phase6.proof import expected_event_count, expected_gold_rows, expected_vault_rows
from fabric_etl_phase6.vault import event_intervals, project_vault, select_new_events, vault_spec
from test_fabric_sales_vault import project


pytestmark = pytest.mark.skipif(
    os.environ.get("CONTOSO_SPARK_TESTS") != "1",
    reason="Real local Spark tests require explicit CONTOSO_SPARK_TESTS=1 and a working Java runtime",
)


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    session = (SparkSession.builder.master("local[2]").appName("contoso-phase6-tests")
               .config("spark.sql.shuffle.partitions", "2")
               .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def quality_frame(spark, release, source_id, number):
    expected = expected_vault_rows(number)
    rows = {
        "SALES.CUSTOMERS": {
            **expected["HUB_CUSTOMER"], **expected["SAT_CUSTOMER"],
        },
        "SALES.PRODUCTS": {
            **expected["HUB_PRODUCT"], **expected["SAT_PRODUCT"],
            "unit_margin": Decimal("2.0000") if number == 1 else Decimal("4.0000"),
        },
        "SALES.EMPLOYEES": {
            **expected["HUB_EMPLOYEE"], **expected["SAT_EMPLOYEE"],
        },
        "SALES.ORDERS": {
            **expected["SAT_ORDER"], **expected["SAT_ORDER_LINE"],
            "order_key": "P6-O1", "order_line_number": 1, "customer_key": "P6-C1",
            "product_key": "P6-P1", "employee_key": "P6-E1",
        },
        "SALES.RETURNS": {
            **expected["SAT_RETURN"], "return_key": "P6-R1", "order_key": "P6-O1",
            "order_line_number": 1, "product_key": "P6-P1", "employee_key": "P6-E1",
        },
    }
    schema_id = next(source["schema_id"] for source in release.document["source_objects"] if source["id"] == source_id)
    columns = next(schema["columns"] for schema in release.document["schemas"] if schema["id"] == schema_id)
    from pyspark.sql import functions as F

    frame = spark.createDataFrame([{column["name"]: rows[source_id][column["name"]] for column in columns}], spark_schema(columns))
    return frame.withColumn("project_id", F.lit("contoso")).withColumn(
        "batch_id", F.lit("batch-" + str(number)),
    ).withColumn("load_date_utc", F.lit(f"2026-10-12T06:00:0{number}+00:00"))


def test_real_spark_publication_insert_select_evaluates_actual_encoded_literals(spark, monkeypatch):
    from test_fabric_sales_product_publication import Control, receipt, setup_pointer
    from fabric_etl_phase6.publication import publish_gold

    _, registered = setup_pointer(monkeypatch)
    statements = []

    class Sql:
        def sql(self, sql):
            statements.append(sql)
            assert " VALUES " not in sql
            query = "SELECT " + sql.split(" SELECT ", 1)[1]
            self.result = spark.sql(query).collect()
            assert self.result[0][0] == "contoso"
            assert self.result[0][1] == "PHASE6_SALES_MART"
            return self

        def collect(self):
            return []

    control = Control()
    control.spark = Sql()

    def read(_spark, sql):
        if "active_product" in sql:
            return [{"active_release_id": "new", "generation": 1}] if len(statements) == 2 else []
        return [registered] if statements else []

    monkeypatch.setattr("fabric_etl_phase6.publication.rows", read)
    result = publish_gold(
        control, "owner", project=project(), receipt=receipt(), batch_id="batch",
        now=datetime.now(timezone.utc), proof_only=True,
    )
    assert result == {"status": "ACTIVATED", "active_release_id": "new", "generation": 1}
    assert len(statements) == 2


def test_real_spark_projects_every_sales_raw_and_business_entity_and_selects_only_actual_events(spark):
    from pyspark.sql import functions as F

    release = project()
    histories = {}
    for number in (1, 2, 3):
        for entity_id, expected in expected_vault_rows(number).items():
            business = entity_id.endswith("_BV")
            spec = vault_spec(release, entity_id, business=business)
            staged = project_vault(quality_frame(spark, release, spec["source_id"], number),
                                   release, entity_id, business=business).cache()
            try:
                row = staged.first().asDict()
                assert {column: row[column] for column in expected} == expected
                if spec["hash_columns"]:
                    types = {mapping["target_column"]: mapping["data_type"] for mapping in spec["mappings"]}
                    assert row["hashdiff"] == hash_key("HASHDIFF", [
                        scalar(expected[column], types[column]) for column in spec["hash_columns"]
                    ])
                previous = histories.get(entity_id, staged.limit(0).withColumn("_event_id", F.lit("").cast("string")))
                events = select_new_events(
                    staged, previous, key=spec["key_column"], satellite=spec["entity"]["type"] not in {"HUB", "LINK"},
                ).withColumn("_event_id", F.sha2(F.to_json(F.struct(spec["key_column"], "batch_id")), 256))
                assert events.count() == expected_event_count(entity_id, number)
                histories[entity_id] = previous.unionByName(events).localCheckpoint(eager=True)
            finally:
                staged.unpersist()
    assert histories["SAT_CUSTOMER"].count() == 3
    assert histories["SAT_ORDER_LINE"].count() == 1
    intervals = event_intervals(histories["SAT_CUSTOMER"], key="hk_customer").orderBy("load_date").collect()
    assert [row["is_current"] for row in intervals] == [False, False, True]
    assert intervals[0]["load_end_date"] == intervals[1]["load_date"]
    assert intervals[1]["load_end_date"] == intervals[2]["load_date"]
    assert intervals[2]["is_deleted"] is True
    cutoff = datetime(2026, 10, 12, 6, 0, 2, tzinfo=timezone.utc)
    pit = project_pit(histories["HUB_CUSTOMER"], histories["SAT_CUSTOMER"], histories["SAT_CUSTOMER_BV"],
                      project_id="contoso", batch_id="pit-2", as_of=cutoff).first()
    expected_raw = histories["SAT_CUSTOMER"].where(F.col("batch_id") == "batch-2").first()
    expected_bv = histories["SAT_CUSTOMER_BV"].first()
    assert pit["raw_event_id"] == expected_raw["_event_id"]
    assert pit["bv_event_id"] == expected_bv["_event_id"]
    assert pit["raw_load_date"] == expected_raw["load_date"]
    assert pit["bv_load_date"] == expected_bv["load_date"]
    with pytest.raises(ContractError, match="lacks a committed satellite"):
        project_pit(histories["HUB_CUSTOMER"], histories["SAT_CUSTOMER"].limit(0), histories["SAT_CUSTOMER_BV"],
                    project_id="contoso", batch_id="missing", as_of=cutoff)
    with pytest.raises(ContractError, match="ambiguous"):
        project_pit(histories["HUB_CUSTOMER"], histories["SAT_CUSTOMER"].unionByName(histories["SAT_CUSTOMER"]),
                    histories["SAT_CUSTOMER_BV"], project_id="contoso", batch_id="duplicate", as_of=cutoff)
    root = Path(__file__).resolve().parents[1] / "projects" / "contoso"
    sources = {entity["expression_ref"]: (root / ("sql/" + entity["expression_ref"] + ".sql")).read_bytes().decode()
               for entity in release.document["gold_entities"] if entity["source_system_id"] == "SALES"}
    gold, hashes = project_gold(spark, project=release, states=histories, sources=sources)
    expected_counts = {
        "GH_DIM_CUSTOMER": 3, "GH_DIM_PRODUCT": 3, "GH_DIM_EMPLOYEE": 2,
        "GH_FCT_SALES": 1, "GH_FCT_RETURNS": 1,
        "GC_DIM_CUSTOMER": 0, "GC_DIM_PRODUCT": 0, "GC_DIM_EMPLOYEE": 0,
        "GC_FCT_SALES": 1, "GC_FCT_RETURNS": 1, "GC_DIM_DATE": 4018,
    }
    assert {entity: frame.count() for entity, frame in gold.items()} == expected_counts
    sales = gold["GH_FCT_SALES"].select(
        "net_amount", "gross_amount", "order_status", "customer_hk", "employee_hk",
    ).first()
    assert sales["net_amount"] == Decimal("20.0000")
    assert sales["gross_amount"] == Decimal("20.0000")
    assert sales["order_status"] == "DELIVERED"
    assert sales["customer_hk"] == expected_vault_rows(1)["HUB_CUSTOMER"]["hk_customer"]
    assert sales["employee_hk"] == expected_vault_rows(1)["HUB_EMPLOYEE"]["hk_employee"]
    assert len(hashes) == 11
    changed = dict(histories)
    changed["SAT_ORDER"] = histories["SAT_ORDER"].unionByName(
        histories["SAT_ORDER"].withColumn("order_status", F.lit("CANCELLED")).withColumn(
            "load_date", F.lit(datetime(2026, 10, 13, tzinfo=timezone.utc)),
        ).withColumn("batch_id", F.lit("late-order-status")).withColumn("_event_id", F.lit("late-order-status-event")),
    )
    changed["SAT_CUSTOMER_BV"] = histories["SAT_CUSTOMER_BV"].unionByName(
        histories["SAT_CUSTOMER_BV"].withColumn("customer_type", F.lit("B2B")).withColumn(
            "load_date", F.lit(datetime(2026, 10, 13, tzinfo=timezone.utc)),
        ).withColumn("batch_id", F.lit("bv-only")).withColumn("_event_id", F.lit("bv-only-event")),
    )
    changed_gold, _ = project_gold(spark, project=release, states=changed, sources=sources)
    versions = changed_gold["GH_FCT_SALES"].orderBy("valid_from").select(
        "order_status", "is_current", F.col("valid_from").cast("string").alias("valid_from"),
        F.col("valid_to").cast("string").alias("valid_to"),
    ).collect()
    assert [row["order_status"] for row in versions] == ["DELIVERED", "CANCELLED"]
    assert versions[0]["valid_to"] == versions[1]["valid_from"]
    assert versions[0]["is_current"] is False
    assert versions[1]["is_current"] is True
    customers = changed_gold["GH_DIM_CUSTOMER"].orderBy("valid_from").select("customer_type").collect()
    assert [row["customer_type"] for row in customers] == ["B2C", "B2C", "B2C", "B2B"]


def test_real_spark_rejects_conflicting_payloads_backdating_and_null_required_mapping(spark):
    from pyspark.sql import functions as F

    release = project()
    source = quality_frame(spark, release, "SALES.ORDERS", 1)
    with pytest.raises(ContractError, match="Conflicting business payloads"):
        project_vault(source.unionByName(source.withColumn("quantity", F.lit(3))),
                      release, "SAT_ORDER_LINE")
    customers = quality_frame(spark, release, "SALES.CUSTOMERS", 1)
    with pytest.raises(ContractError, match="required identity column"):
        project_vault(customers.withColumn("customer_key", F.lit(None).cast("string")), release, "HUB_CUSTOMER")
    staged = project_vault(customers, release, "SAT_CUSTOMER")
    with pytest.raises(ContractError, match="backdate"):
        select_new_events(staged, staged, key="hk_customer", satellite=True)
    with pytest.raises(ContractError, match="unique key/timestamp"):
        event_intervals(staged.unionByName(staged), key="hk_customer")


def expected_states(spark, release):
    from pyspark.sql import types as T

    states = {}
    for entity_id in expected_vault_rows(1):
        spec = vault_spec(release, entity_id, business=entity_id.endswith("_BV"))
        payload = spark_schema(spec["schema_columns"])
        if spec["key_column"] not in payload.names:
            payload.add(spec["key_column"], T.StringType(), False)
        fields = [
            T.StructField("project_id", T.StringType(), False),
            T.StructField("batch_id", T.StringType(), False),
            T.StructField("load_date", T.TimestampType(), False),
            T.StructField("record_source", T.StringType(), False),
            *payload.fields,
            T.StructField("_event_id", T.StringType(), False),
        ]
        if spec["hash_columns"]:
            fields.append(T.StructField("hashdiff", T.StringType(), False))
        values = []
        for number in (1, 2, 3):
            if not expected_event_count(entity_id, number):
                continue
            expected = expected_vault_rows(number)[entity_id]
            row = {
                **expected, "project_id": "contoso", "batch_id": "batch-" + str(number),
                "load_date": datetime(2026, 10, 12, 6, 0, number, tzinfo=timezone.utc),
                "record_source": "SALES", "_event_id": entity_id + "-" + str(number),
            }
            if spec["hash_columns"]:
                types = {mapping["target_column"]: mapping["data_type"] for mapping in spec["mappings"]}
                row["hashdiff"] = hash_key("HASHDIFF", [
                    scalar(expected[column], types[column]) for column in spec["hash_columns"]
                ])
            values.append(row)
        states[entity_id] = spark.createDataFrame(values, T.StructType(fields)).cache()
    return states


def test_real_spark_gold_full_business_outputs_and_multistream_history_from_independent_states(spark):
    from pyspark.sql import functions as F

    release = project()
    states = expected_states(spark, release)
    root = Path(__file__).resolve().parents[1] / "projects" / "contoso"
    sources = {entity["expression_ref"]: (root / "sql" / (entity["expression_ref"] + ".sql")).read_bytes().decode()
               for entity in sales_gold_entities(release)}
    try:
        gold, _ = project_gold(spark, project=release, states=states, sources=sources)
        times = [datetime(2026, 10, 12, 6, 0, number, tzinfo=timezone.utc) for number in (1, 2, 3)]
        expected = expected_gold_rows(3, times)
        for entity in sales_gold_entities(release):
            entity_id = entity["id"]
            schema = spark_schema(gold_columns(release, entity, business_only=True))
            values = [
                {key: value.replace(tzinfo=timezone.utc) if isinstance(value, datetime) and value.tzinfo is None else value
                 for key, value in row.items()}
                for row in expected[entity_id]
            ]
            frame = spark.createDataFrame(values, schema)
            missing = frame.exceptAll(gold[entity_id]).limit(3).toJSON().collect()
            unexpected = gold[entity_id].exceptAll(frame).limit(3).toJSON().collect()
            assert not missing and not unexpected, (entity_id, missing, unexpected)
        changed = dict(states)
        changed["SAT_ORDER"] = states["SAT_ORDER"].unionByName(
            states["SAT_ORDER"].withColumn("order_status", F.lit("CANCELLED")).withColumn(
                "load_date", F.lit(datetime(2026, 10, 13, tzinfo=timezone.utc)),
            ).withColumn("batch_id", F.lit("late-order-status")).withColumn("_event_id", F.lit("late-order-status-event")),
        )
        changed["SAT_CUSTOMER_BV"] = states["SAT_CUSTOMER_BV"].unionByName(
            states["SAT_CUSTOMER_BV"].withColumn("customer_type", F.lit("B2B")).withColumn(
                "load_date", F.lit(datetime(2026, 10, 13, tzinfo=timezone.utc)),
            ).withColumn("batch_id", F.lit("bv-only")).withColumn("_event_id", F.lit("bv-only-event")),
        )
        temporal_gold, _ = project_gold(spark, project=release, states=changed, sources=sources)
        sales = temporal_gold["GH_FCT_SALES"].orderBy("valid_from").select(
            "order_status", "is_current", F.col("valid_from").cast("string").alias("valid_from"),
            F.col("valid_to").cast("string").alias("valid_to"),
        ).collect()
        assert [row["order_status"] for row in sales] == ["DELIVERED", "CANCELLED"]
        assert sales[0]["valid_to"] == sales[1]["valid_from"]
        assert [row["is_current"] for row in sales] == [False, True]
        customers = temporal_gold["GH_DIM_CUSTOMER"].orderBy("valid_from").select("customer_type").collect()
        assert [row["customer_type"] for row in customers] == ["B2C", "B2C", "B2C", "B2B"]
        cutoff = datetime(2026, 10, 12, 6, 0, 2, tzinfo=timezone.utc)
        pit = project_pit(states["HUB_CUSTOMER"], states["SAT_CUSTOMER"], states["SAT_CUSTOMER_BV"],
                          project_id="contoso", batch_id="pit", as_of=cutoff).first()
        assert pit["raw_event_id"] == "SAT_CUSTOMER-2"
        assert pit["bv_event_id"] == "SAT_CUSTOMER_BV-1"
        with pytest.raises(ContractError, match="matching as-of hub"):
            project_pit(states["HUB_CUSTOMER"], states["SAT_CUSTOMER"].withColumn("hk_customer", F.lit("orphan")),
                        states["SAT_CUSTOMER_BV"], project_id="contoso", batch_id="orphan", as_of=cutoff)
    finally:
        for state in states.values():
            state.unpersist()
