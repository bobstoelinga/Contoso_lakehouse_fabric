# Fabric notebook source

# METADATA ********************
# META {
# META   "kernel_info": {"name": "synapse_pyspark"},
# META   "dependencies": {}
# META }

# PARAMETERS CELL ********************
environment_json = "{}"
project_json = "{}"
expected_phase6_version = "0.1.0"

# METADATA ********************
# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************
import json
from datetime import datetime, timezone
from decimal import Decimal
from uuid import uuid4

import notebookutils
from pyspark.sql import functions as F, types as T

from fabric_etl.contracts import Environment, canonical_json, fingerprint
from fabric_etl.control import ControlStore, literal, rows
from fabric_etl.project import ProjectRelease
from fabric_etl_phase5.delivery import _file_identity, ingest_registered_delivery
from fabric_etl_phase6 import __version__
from fabric_etl_phase6.fixtures import sales_fixture
from fabric_etl_phase6.quality import quality_order, required_references, write_quality

if __version__ != expected_phase6_version:
    raise RuntimeError("Sales proof requires its exact published phase-six runtime")
environment = Environment.from_dict(json.loads(environment_json))
project = ProjectRelease(json.loads(project_json), (), ("UNACTIVATED_CANDIDATE",))
spark.conf.set("spark.sql.session.timeZone", "UTC")
control = ControlStore(spark, environment)
fs = notebookutils.fs
base = (f"abfss://{environment.workspace_name}@onelake.dfs.fabric.microsoft.com/"
        f"{environment.lakehouses['processing'].name}.Lakehouse/Files/phase6-proof/sales-quality-v1")
owner = control.claim("phase6-sales-quality:" + uuid4().hex, now=datetime.now(timezone.utc))
reports = []
try:
    for number in (1, 2, 3):
        fixture = sales_fixture(number)
        source_ids = set(fixture)
        root = base + "/" + str(number)
        delivery_id = "PHASE6_SALES_QUALITY|" + str(number)
        fs.mkdirs(root)
        present = {item.name for item in fs.ls(root)}
        if present - {"_manifest.json", *[key.split(".")[1].lower() + ".parquet" for key in source_ids]}:
            raise RuntimeError("Sales proof folder has an unexpected inventory")
        objects, schemas, targets = [], {}, {}
        for source_id, values in fixture.items():
            name = source_id.split(".")[1].lower()
            schema = T.StructType([T.StructField(column, T.StringType(), True) for column in values[0]])
            frame = spark.createDataFrame(values, schema)
            path = root + "/" + name + ".parquet"
            if name + ".parquet" not in present:
                temporary = root + "_" + name + "_staging_" + uuid4().hex
                frame.coalesce(1).write.mode("errorifexists").parquet(temporary)
                files = [item.path for item in fs.ls(temporary) if item.name.endswith(".parquet")]
                if len(files) != 1 or not fs.mv(files[0], path, False):
                    raise RuntimeError("Sales Parquet fixture creation failed")
                if not fs.rm(temporary, True):
                    raise RuntimeError("Sales fixture staging cleanup failed")
            observed = spark.read.schema(schema).parquet(path)
            if frame.exceptAll(observed).limit(1).count() or observed.exceptAll(frame).limit(1).count():
                raise RuntimeError("Immutable Sales fixture payload differs")
            identity = _file_identity(spark, path)
            objects.append({
                "source_object_id": source_id,
                "files": [{"path": name + ".parquet", "size_bytes": identity["length"], "sha256": identity["sha256"]}],
                "complete_snapshot": source_id in {"SALES.CUSTOMERS", "SALES.PRODUCTS", "SALES.EMPLOYEES"},
                "empty_snapshot": False,
            })
            schemas[source_id] = schema
            targets[source_id] = "phase5_exit_phase6_sales_" + name
        existing = rows(spark, f"SELECT * FROM {control.table('delivery')} "
                        f"WHERE project_id = {literal(control.project)} AND source_system_id = 'PHASE4_PROOF'")
        matching = [item for item in existing if item["delivery_id"] == delivery_id]
        previous = rows(spark, f"SELECT MAX(delivery_sequence) AS sequence FROM {control.table('batch_context')} "
                        f"WHERE project_id = {literal(control.project)} AND source_system_id = 'PHASE4_PROOF'")
        sequence = matching[0]["delivery_sequence"] if matching else max(
            [item["delivery_sequence"] for item in existing] + [previous[0]["sequence"] or 0],
        ) + 1
        document = {
            "contract_version": "1.0", "project_id": control.project, "source_system_id": "PHASE4_PROOF",
            "delivery_id": delivery_id, "delivery_sequence": sequence,
            "schema_contract_version": project.release_id, "status": "CLOSED",
            "objects": sorted(objects, key=lambda item: item["source_object_id"]),
        }
        manifest_path = root + "/_manifest.json"
        text = canonical_json(document).decode()
        if "_manifest.json" not in present and not fs.put(manifest_path, text, False):
            raise RuntimeError("Sales manifest creation failed")
        if fs.head(manifest_path, max_bytes=len(text.encode()) + 1) != text:
            raise RuntimeError("Immutable Sales manifest differs")
        ingest = ingest_registered_delivery(
            control, owner, fs=fs, root=root, document=document, required_objects=source_ids,
            source_formats={key: "PARQUET" for key in source_ids}, schemas=schemas,
            targets=targets, now=datetime.now(timezone.utc), proof_only=True,
        )
        quality = {}
        for source_id in quality_order(project, source_ids):
            receipt = write_quality(
                control, owner, project=project, source_id=source_id, source_schema=schemas[source_id],
                ingest=ingest, bronze_table=targets[source_id],
                reference_commits={key: quality[key] for key in required_references(project, source_id)},
                table="phase6_sales_" + source_id.split(".")[1].lower(),
                now=datetime.now(timezone.utc), proof_only=True,
            )
            if receipt["accounting"]["counts"] != {"PASSED": 1, "REJECTED": 0, "FILTERED": 0}:
                raise RuntimeError("Actual Sales metadata rejected the valid scenario: " + source_id)
            actual = spark.table(receipt["outputs"]["PASSED"]["target_table"]).where(
                (F.col("project_id") == control.project) & (F.col("batch_id") == ingest["batch_id"]),
            ).select("typed_payload_json").collect()
            expected = {
                "SALES.CUSTOMERS": {
                    "customer_key": "P6-C1", "customer_name": "Customer One" if number == 1 else "Customer Changed",
                    "email": None, "phone": "+31123", "address_line1": "street 1", "city": "Utrecht",
                    "state_province": "UT", "postal_code": "1234AB", "country_code": "NL",
                    "customer_segment": "RETAIL", "customer_since": "2020-01-01", "is_deleted": number == 3,
                },
                "SALES.PRODUCTS": {
                    "product_key": "P6-P1", "product_name": "product one", "product_category": "CAT",
                    "product_subcategory": "SUB", "brand": "brand", "unit_cost": Decimal("8.0000"),
                    "unit_price": Decimal("10.0000") if number == 1 else Decimal("12.0000"),
                    "unit_margin": Decimal("2.0000") if number == 1 else Decimal("4.0000"),
                    "is_discontinued": False, "is_deleted": number == 3,
                },
                "SALES.EMPLOYEES": {
                    "employee_key": "P6-E1", "first_name": "First", "last_name": "Last", "job_title": "sales",
                    "office_city": "Utrecht", "hire_date": "2020-01-01", "is_deleted": number == 3,
                },
                "SALES.ORDERS": {
                    "order_key": "P6-O1", "order_line_number": 1, "customer_key": "P6-C1", "product_key": "P6-P1",
                    "employee_key": "P6-E1", "order_date": "2026-10-10", "ship_date": "2026-10-11",
                    "delivery_date": "2026-10-11", "order_status": "DELIVERED", "quantity": 2,
                    "unit_price": Decimal("10.0000"), "discount_amount": Decimal("0.0000"),
                    "net_amount": Decimal("20.0000"), "currency_code": "USD",
                },
                "SALES.RETURNS": {
                    "return_key": "P6-R1", "order_key": "P6-O1", "order_line_number": 1, "product_key": "P6-P1",
                    "employee_key": "P6-E1", "return_date": "2026-10-11", "return_status": "ACCEPTED",
                    "return_reason_code": "DAMAGED", "return_quantity": 1, "refund_amount": Decimal("10.0000"),
                    "currency_code": "USD",
                },
            }[source_id]
            if len(actual) != 1 or json.loads(actual[0]["typed_payload_json"], parse_float=Decimal) != expected:
                raise RuntimeError("Sales output differs from independent expected values: " + source_id)
            receipt["expected_output_verified"] = True
            quality[source_id] = receipt
        reports.append({"delivery_number": number, "fixture_fingerprint": fingerprint(fixture),
                        "ingest": ingest, "quality": quality})
finally:
    control.release(owner)
notebookutils.notebook.exit(json.dumps({
    "status": "SUCCEEDED", "scope": "PHASE6_SALES_COMMITTED_QUALITY",
    "phase6_version": __version__, "release_id": project.release_id, "deliveries": reports,
    "owner_released": True, "business_data_written": False, "production_release_accepted": False,
    "phase6_complete": False,
}, sort_keys=True))
