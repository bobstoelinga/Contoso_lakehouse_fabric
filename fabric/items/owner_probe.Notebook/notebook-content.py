# Fabric notebook source

# METADATA ********************
# META {
# META   "kernel_info": {"name": "synapse_pyspark"},
# META   "dependencies": {}
# META }

# PARAMETERS CELL ********************
environment_json = ""
expected_framework_version = "0.1.0"
mode = ""
start_utc = ""
owner_json = ""
terminal_proof_json = ""

# METADATA ********************
# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************
import json
import notebookutils

from fabric_etl import __version__
from fabric_etl.control import ControlSpark
from fabric_etl.contracts import ContractError
from fabric_etl.proof import owner_probe

spark: ControlSpark

if expected_framework_version != __version__:
    raise ContractError("Installed framework version differs")
result = owner_probe(
    spark, environment_json, notebookutils.runtime.context["activityId"],
    mode, start_utc, owner_json, terminal_proof_json,
)
print(json.dumps(result, sort_keys=True))
notebookutils.notebook.exit(json.dumps(result, sort_keys=True))

# METADATA ********************
# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
