# Fabric notebook source

# METADATA ********************
# META {
# META   "kernel_info": {"name": "synapse_pyspark"},
# META   "dependencies": {}
# META }

# PARAMETERS CELL ********************
environment_json = ""
expected_framework_version = "0.1.0"

# METADATA ********************
# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************
import json
import notebookutils

from fabric_etl.runtime import SparkSession, verify_foundation

spark: SparkSession

result = verify_foundation(spark, environment_json, expected_framework_version)
print(json.dumps(result, sort_keys=True))
notebookutils.notebook.exit(json.dumps(result, sort_keys=True))

# METADATA ********************
# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
