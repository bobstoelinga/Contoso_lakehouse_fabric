# Fabric notebook source

# METADATA ********************
# META {
# META   "kernel_info": {"name": "synapse_pyspark"},
# META   "dependencies": {}
# META }

# PARAMETERS CELL ********************
environment_json = "{}"
project_json = "{}"
expected_phase5_version = "0.4.0"

# METADATA ********************
# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************
import json
from datetime import datetime, timezone

import notebookutils
from pyspark.sql import functions as F, types as T

from fabric_etl.contracts import ContractError
from fabric_etl.project import ProjectRelease
from fabric_etl_phase5 import __version__
from fabric_etl_phase5.ingestion import RAW_RECORD_COLUMNS, spark_schema
from fabric_etl_phase5.mapping import evaluate_metadata_quality, quality_spec
from fabric_etl_phase5.quality import QualityBlocked, frame_fingerprint

if __version__ != expected_phase5_version:
    raise RuntimeError("Metadata runtime differs from its immutable bundle pin")
document = json.loads(project_json)
# The deployment planner validates all files and fingerprints before embedding this unactivated candidate.
project = ProjectRelease(document, (), ("UNACTIVATED_CANDIDATE",))
fixture = {
    "SALES.CUSTOMERS": {
        "customer_key": "C1", "customer_name": "customer one", "email": None, "phone": "+31123",
        "address_line1": "street 1", "city": "utrecht", "state_province": "ut",
        "postal_code": "1234AB", "country": "nl", "customer_segment": "retail",
        "customer_since": "2020-01-01", "is_deleted": "false",
    },
    "SALES.PRODUCTS": {
        "product_key": "P1", "product_name": "product one", "product_category": "cat",
        "product_subcategory": "sub", "brand": "brand", "unit_cost": "8.0",
        "unit_price": "10.0", "is_discontinued": "false", "is_deleted": "false",
    },
    "SALES.EMPLOYEES": {
        "employee_key": "E1", "first_name": "first", "last_name": "last",
        "job_title": "sales", "office_city": "utrecht", "hire_date": "2020-01-01", "is_deleted": "false",
    },
    "SALES.ORDERS": {
        "order_key": "O1", "order_line_number": "1", "customer_key": "C1", "product_key": "P1",
        "employee_key": "E1", "order_date": "2026-10-10", "ship_date": "2026-10-11",
        "delivery_date": "2026-10-11", "order_status": "delivered", "quantity": "2",
        "unit_price": "10.0", "discount_amount": "0", "net_amount": "20.0", "currency_code": "usd",
    },
    "SALES.RETURNS": {
        "return_key": "R1", "order_key": "O1", "order_line_number": "1", "product_key": "P1",
        "employee_key": "E1", "return_date": "2026-10-11", "return_status": "accepted",
        "return_reason_code": "damaged", "return_quantity": "1", "refund_amount": "10.0", "currency_code": "usd",
    },
    "CBS.JEUGDZORG_WIJK_2025": {
        "Vormen": "V1", "Wijken": "W1", "Perioden": "2025",
        "TotaalJongerenMetJeugdzorgInNatur_1": "10", "TotaalTrajectenInNatur_9": "20",
        "Gemeentenaam_18": "Utrecht", "SoortRegio_19": "wijk",
    },
    "ECB.EXCHANGE_RATE": {
        "CURRENCY": "USD", "TIME_PERIOD": "2026-10-10", "OBS_VALUE": "1.1", "FREQ": "D",
        "CURRENCY_DENOM": "EUR", "EXR_TYPE": "SP00", "EXR_SUFFIX": "A",
    },
    "NAGER.HOLIDAYS_NL": {
        "date": "2026-01-01", "name": "New Year's Day", "localName": "Nieuwjaarsdag",
        "countryCode": "NL", "global": "true", "types": ["Public"],
    },
    "FABRIC_SALES.ORDER_LINES": {
        "sales_order_detail_id": "1", "order_quantity": "2", "unit_price": "10.0",
        "unit_price_discount": "0.0", "subtotal_amount": "20.0", "tax_amount": "2.0",
        "freight_amount": "1.0", "order_status_code": "5",
        "source_last_modified_at": "2026-10-10T12:00:00Z", "is_deleted": "false",
    },
}
schemas = {
    source_id: T.StructType([
        T.StructField(name, T.ArrayType(T.StringType()) if name == "types" else T.StringType(), True)
        for name in payload
    ]) for source_id, payload in fixture.items()
}


def records_for(source_id, payloads):
    values = []
    for index, payload in enumerate(payloads):
        row = {name: "metadata-fixture" for name in RAW_RECORD_COLUMNS}
        row.update(
            project_id=document["project_id"], source_system_id=source_id.split(".")[0],
            source_object_id=source_id, delivery_id="METADATA_FIXTURE_V1", batch_id="metadata-fixture-batch",
            release_id=project.release_id, raw_record_id=f"{source_id}-{index}",
            payload_json=json.dumps(payload), delivery_sequence=1, duplicate_ordinal=index + 1,
            load_date_utc="2026-10-11T00:00:00Z",
        )
        values.append(tuple(row[name] for name in RAW_RECORD_COLUMNS))
    return spark.createDataFrame(values, list(RAW_RECORD_COLUMNS))


references, reports = {}, {}
for source_id in fixture:
    needed = quality_spec(
        project, source_id, reference_views={key: "unused" for key in references},
    )["reference_sources"]
    outcomes, accounting = evaluate_metadata_quality(
        records_for(source_id, [fixture[source_id]]), source_schema=schemas[source_id],
        project=project, source_id=source_id, reference_frames={key: references[key] for key in needed},
    )
    if accounting["counts"] != {"PASSED": 1, "REJECTED": 0, "FILTERED": 0}:
        raise RuntimeError(f"Actual metadata rejected the valid fixture: {source_id}: {accounting}")
    spec = quality_spec(project, source_id, reference_views={key: "unused" for key in references})
    typed = outcomes.withColumn(
        "_typed", F.from_json("typed_payload_json", spark_schema(spec["columns"])),
    ).select(*outcomes.columns, "_typed.*")
    references[source_id] = typed
    reports[source_id] = {"accounting": accounting, "fingerprint": frame_fingerprint(outcomes, "raw_record_id")}

if sum(len(report["accounting"]["metadata_rule_ids"]) for report in reports.values()) != 33:
    raise RuntimeError("The executed metadata rule inventory differs from all 33 actual rules")

ecb = fixture["ECB.EXCHANGE_RATE"]
filtered, accounting = evaluate_metadata_quality(
    records_for("ECB.EXCHANGE_RATE", [ecb, dict(ecb, OBS_VALUE="missing")]),
    source_schema=schemas["ECB.EXCHANGE_RATE"], project=project, source_id="ECB.EXCHANGE_RATE", reference_frames={},
)
if accounting["counts"] != {"PASSED": 1, "REJECTED": 0, "FILTERED": 1}:
    raise RuntimeError("ECB source KEEP filter did not run before typed Quality rules")

invalid, invalid_accounting = evaluate_metadata_quality(
    records_for("SALES.CUSTOMERS", [dict(fixture["SALES.CUSTOMERS"], is_deleted="not-a-boolean")]),
    source_schema=schemas["SALES.CUSTOMERS"], project=project, source_id="SALES.CUSTOMERS", reference_frames={},
)
if invalid_accounting["counts"] != {"PASSED": 0, "REJECTED": 1, "FILTERED": 0}:
    raise RuntimeError("Coalesced invalid boolean was silently accepted")
if not any(code.startswith("INVALID_CAST_") for code in invalid.first()["error_codes"]):
    raise RuntimeError("Rejected boolean has no explicit cast diagnostic")

duplicate_blocked = False
try:
    evaluate_metadata_quality(
        records_for("SALES.CUSTOMERS", [fixture["SALES.CUSTOMERS"], fixture["SALES.CUSTOMERS"]]),
        source_schema=schemas["SALES.CUSTOMERS"], project=project, source_id="SALES.CUSTOMERS", reference_frames={},
    )
except QualityBlocked as exc:
    if "DQ-CUST-002:FAIL_BATCH" not in str(exc):
        raise
    duplicate_blocked = True
if not duplicate_blocked:
    raise RuntimeError("Actual metadata duplicate threshold did not block")

retry, retry_metrics = evaluate_metadata_quality(
    records_for("SALES.CUSTOMERS", [fixture["SALES.CUSTOMERS"]]),
    source_schema=schemas["SALES.CUSTOMERS"], project=project, source_id="SALES.CUSTOMERS", reference_frames={},
)
if frame_fingerprint(retry, "raw_record_id") != tuple(reports["SALES.CUSTOMERS"]["fingerprint"]):
    raise RuntimeError("Metadata evaluation changed its fingerprint on identical input")
notebookutils.notebook.exit(json.dumps({
    "status": "SUCCEEDED", "scope": "PHASE5_ACTUAL_METADATA_QUALITY_FIXTURES",
    "phase5_version": __version__, "release_id": project.release_id, "sources": reports,
    "ecb_filter_accounting": accounting, "invalid_cast_accounting": invalid_accounting,
    "duplicate_threshold_blocked": duplicate_blocked, "retry_fingerprint_equal": True,
    "business_data_written": False, "production_release_accepted": False,
}, sort_keys=True))
