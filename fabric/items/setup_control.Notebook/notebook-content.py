# Fabric notebook source

# METADATA ********************
# META {
# META   "kernel_info": {"name": "synapse_pyspark"},
# META   "dependencies": {}
# META }

# PARAMETERS CELL ********************
environment_json = ""
expected_framework_version = "0.1.0"
migrations_json = ""
project_json = ""
files_json = ""

# METADATA ********************
# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************
import json
import notebookutils

from fabric_etl.proof import setup_proof
from fabric_etl.control import ControlSpark

spark: ControlSpark

activity_id = notebookutils.runtime.context["activityId"]
result = setup_proof(
    spark, environment_json, expected_framework_version,
    migrations_json, project_json, files_json, activity_id,
)
print(json.dumps(result, sort_keys=True))
notebookutils.notebook.exit(json.dumps(result, sort_keys=True))

# METADATA ********************
# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
