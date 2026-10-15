"""Thin runtime entrypoints; they do not declare the full ETL implemented."""

from __future__ import annotations

from typing import Protocol

from fabric_etl import __version__
from fabric_etl.contracts import ContractError, Environment, parse_json


class SparkResult(Protocol):
    def collect(self) -> list: ...


class SparkSession(Protocol):
    def sql(self, query: str) -> SparkResult: ...


def verify_foundation(
    spark: SparkSession, environment_json: str, expected_framework_version: str,
) -> dict:
    if expected_framework_version != __version__:
        raise ContractError("Installed framework version differs from the pinned release")
    environment = Environment.from_dict(parse_json(environment_json))
    environment.require_provisioned()
    roles = []
    for role, binding in environment.lakehouses.items():
        namespace = f"`{environment.workspace_name}`.`{binding.name}`.`dbo`"
        # Fabric resolves schema namespaces, not lakehouse parents, for SHOW.
        spark.sql(f"SHOW TABLES IN {namespace}").collect()
        roles.append(role)
    return {
        "status": "SUCCEEDED", "scope": "READ_ONLY_FOUNDATION_VISIBILITY",
        "framework_version": __version__, "project_id": environment.project_id,
        "roles": roles,
    }
