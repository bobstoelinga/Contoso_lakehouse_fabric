# Fabric notebook source

# METADATA ********************
# META {
# META   "kernel_info": {"name": "synapse_pyspark"},
# META   "dependencies": {}
# META }

# PARAMETERS CELL ********************
environment_json = "{}"
expected_phase5_version = "0.3.0"

# METADATA ********************
# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************
import json

import com.microsoft.sqlserver.jdbc.spark
import notebookutils
from pyspark.errors import AnalysisException
from pyspark.sql import functions as F

from fabric_etl_phase5 import __version__

if __version__ != expected_phase5_version:
    raise RuntimeError("Sales source probe runtime differs from its version pin")
server = "qugtl6ion76ufa4fuwoc6cpmdi-rqr26cxjvolujgkdq6tknur6xu.database.fabric.microsoft.com"
database = "Contoso_database-a2e53891-642a-4a0c-9a7f-c72e1351ff57"
url = (f"jdbc:sqlserver://{server}:1433;database={database};"
       "encrypt=true;trustServerCertificate=false;loginTimeout=30;applicationIntent=ReadOnly;")
source = spark.read.option("url", url).option(
    "query", "SELECT TOP (0) * FROM SalesLT.vw_databricks_sales_order_line",
).option("queryTimeout", "30").mssql()
expected_columns = {
    "sales_order_detail_id", "sales_order_id", "product_id", "customer_id",
    "bill_to_address_id", "ship_to_address_id", "order_quantity", "unit_price",
    "unit_price_discount", "order_date", "due_date", "ship_date", "order_status_code",
    "is_online_order", "subtotal_amount", "tax_amount", "freight_amount",
    "customer_first_name", "customer_last_name", "bill_to_city", "bill_to_postal_code",
    "ship_to_city", "ship_to_postal_code", "product_name", "product_number", "product_color",
    "product_standard_cost", "product_list_price", "product_category_name",
    "product_model_name", "product_description", "source_last_modified_at", "is_deleted",
}
if set(source.columns) != expected_columns or len(source.columns) != 33:
    raise RuntimeError("Fabric SQL source columns differ from the independent SQL schema receipt")
if source.count() != 0:
    raise RuntimeError("TOP(0) SQL source unexpectedly returned business rows")
try:
    source.select(F.expr(
        "cast(coalesce(total_due_amount, subtotal_amount + tax_amount + freight_amount) as decimal(18,4))",
    )).schema
except AnalysisException as exc:
    if exc.getCondition() != "UNRESOLVED_COLUMN.WITH_SUGGESTION":
        raise
    original_mapping_failure = exc.getCondition()
else:
    raise RuntimeError("Original mapping unexpectedly resolved its absent total_due_amount column")
expressions = {
    "sales_order_detail_id": "cast(sales_order_detail_id as bigint)",
    "order_quantity": "cast(order_quantity as int)",
    "unit_price": "cast(unit_price as decimal(18,4))",
    "unit_price_discount": "cast(unit_price_discount as decimal(9,6))",
    "total_due_amount": "cast(subtotal_amount + tax_amount + freight_amount as decimal(18,4))",
    "order_status_code": "upper(trim(order_status_code))",
    "source_last_modified_at": "cast(source_last_modified_at as timestamp)",
    "is_deleted": "coalesce(cast(is_deleted as boolean), false)",
}
projected = source.select(*[F.expr(expression).alias(name) for name, expression in expressions.items()])
expected_types = {
    "sales_order_detail_id": "bigint", "order_quantity": "int",
    "unit_price": "decimal(18,4)", "unit_price_discount": "decimal(9,6)",
    "total_due_amount": "decimal(18,4)", "order_status_code": "string",
    "source_last_modified_at": "timestamp", "is_deleted": "boolean",
}
observed_types = {field.name: field.dataType.simpleString() for field in projected.schema.fields}
if observed_types != expected_types or projected.count() != 0:
    raise RuntimeError("Corrected real-source Quality projection schema differs")
notebookutils.notebook.exit(json.dumps({
    "status": "SUCCEEDED", "scope": "PHASE5_REAL_SALES_SOURCE_SCHEMA",
    "phase5_version": __version__, "source_column_count": 33,
    "source_columns": source.columns, "projection_types": observed_types,
    "original_mapping_failure": original_mapping_failure,
    "corrected_mapping_expression": expressions["total_due_amount"],
    "authentication": "FABRIC_AUTOMATIC_ENTRA_EXECUTING_IDENTITY",
    "business_rows_extracted": False, "business_data_written": False,
    "production_release_accepted": False,
}, sort_keys=True))
