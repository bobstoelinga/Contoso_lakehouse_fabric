"""Allowlisted, checksummed foundation migrations with exact schema readback."""

from __future__ import annotations

import hashlib
import re
from datetime import datetime

from fabric_etl.contracts import ContractError, Environment, fields
from fabric_etl.control import ControlSpark, literal, rows, timestamp


TABLES = {
    ("control", "metadata", "project_release"),
    ("control", "control", "metadata_pointer"),
    ("control", "control", "project_owner"),
    ("control", "control", "delivery"),
    ("control", "control", "batch_context"),
    ("control", "control", "work_item"),
    ("control", "audit", "events"),
    ("control", "control", "commit_manifest"),
    ("control", "control", "schema_migration"),
    ("gold", "publication", "product_release"),
    ("gold", "publication", "active_product"),
    ("gold", "publication", "consumer_catalog"),
}
SCHEMAS = {
    ("control", "metadata"), ("control", "audit"), ("control", "control"),
    ("processing", "bronze"), ("processing", "quality"), ("processing", "reject"),
    ("processing", "raw_vault"), ("processing", "business_vault"),
    ("gold", "historical"), ("gold", "release_data"), ("gold", "publication"),
}


def render(environment: Environment, source: str) -> str:
    for role, binding in environment.lakehouses.items():
        source = source.replace("{" + role + "}", f"`{environment.workspace_name}`.`{binding.name}`")
    if "{" in source or "}" in source:
        raise ContractError("Unregistered migration binding")
    return source


def declarations(environment: Environment, sql: str) -> list[tuple[str, str, list[tuple[str, str, bool]]]]:
    schemas = {
        f"`{environment.workspace_name}`.`{environment.lakehouses[role].name}`.{schema}"
        for role, schema in SCHEMAS
    }
    tables = {
        f"`{environment.workspace_name}`.`{environment.lakehouses[role].name}`.{schema}.{table}"
        for role, schema, table in TABLES
    }
    result = []
    for statement in sql.split(";"):
        statement = statement.strip()
        if not statement:
            continue
        schema = re.fullmatch(r"CREATE SCHEMA IF NOT EXISTS (.+)", statement)
        if schema:
            if schema[1] not in schemas:
                raise ContractError("Migration schema outside registered scope")
            result.append((statement, "", []))
            continue
        table = re.fullmatch(r"CREATE TABLE IF NOT EXISTS (.+?)\s*\((.+)\)\s*USING DELTA",
                             statement, re.DOTALL)
        if not table or table[1] not in tables:
            raise ContractError("Only registered CREATE TABLE/SCHEMA migrations are allowed")
        columns = []
        for declaration in table[2].split(","):
            column = re.fullmatch(r"\s*([a-z_][a-z_0-9]*)\s+(STRING|BIGINT|INT|TIMESTAMP)(\s+NOT NULL)?\s*",
                                  declaration)
            if not column:
                raise ContractError("Unsupported control column declaration")
            kind = {"STRING": "string", "BIGINT": "bigint", "INT": "int", "TIMESTAMP": "timestamp"}[column[2]]
            columns.append((column[1], kind, column[3] is None))
        if len({column[0] for column in columns}) != len(columns):
            raise ContractError("Duplicate migration column")
        result.append((statement, table[1], columns))
    if not result:
        raise ContractError("Empty migration")
    return result


def migrate(
    spark: ControlSpark, environment: Environment, migrations: object,
    *, exclusive_setup: bool, now: datetime,
) -> list[dict]:
    if exclusive_setup is not True:
        raise ContractError("Migrations require exclusive foundation setup, not an ETL writer")
    if not isinstance(migrations, list) or len(migrations) != 3:
        raise ContractError("Phase-four setup requires the exact three registered migrations")
    expected_ids = {"001_namespaces", "002_control_contract", "003_schema_journal"}
    checked = {}
    for value in migrations:
        migration = fields(value, {"id", "source_sql", "source_sha256"}, "Migration input")
        if migration["id"] not in expected_ids or migration["id"] in checked:
            raise ContractError("Unknown or duplicate phase-four migration")
        source = migration["source_sql"]
        if not isinstance(source, str) or hashlib.sha256(source.encode()).hexdigest() != migration["source_sha256"]:
            raise ContractError("Migration source checksum differs")
        sql = render(environment, source)
        checked[migration["id"]] = (migration, sql, declarations(environment, sql))
    namespaces = checked["001_namespaces"][2]
    journal_declarations = checked["003_schema_journal"][2]
    control_declarations = checked["002_control_contract"][2]
    if (
        len(namespaces) != len(SCHEMAS) or any(table for _, table, _ in namespaces)
        or len({statement for statement, _, _ in namespaces}) != len(SCHEMAS)
        or len(journal_declarations) != 1
        or not journal_declarations[0][1].endswith(".control.schema_migration")
        or len(control_declarations) != len(TABLES) - 1
        or any(not table or table.endswith(".control.schema_migration") for _, table, _ in control_declarations)
        or len({table for _, table, _ in control_declarations}) != len(TABLES) - 1
    ):
        raise ContractError("Migration collection does not cover the complete registered control schema")
    # The journal must exist before applying control tables. Setup is exclusively orchestrated.
    for key in ("001_namespaces", "003_schema_journal"):
        for statement, _, _ in checked[key][2]:
            spark.sql(statement).collect()
    journal = environment.spark_table("control", "control", "schema_migration")
    outcomes = []
    for key in ("001_namespaces", "003_schema_journal", "002_control_contract"):
        migration, sql, statements = checked[key]
        digest = hashlib.sha256(sql.encode()).hexdigest()
        condition = f"project_id = {literal(environment.project_id)} AND migration_id = {literal(key)}"
        existing = rows(spark, f"SELECT * FROM {journal} WHERE {condition}")
        if existing and (
            len(existing) != 1 or existing[0]["source_sha256"] != migration["source_sha256"]
            or existing[0]["rendered_sha256"] != digest or existing[0]["status"] != "APPLIED"
        ):
            raise ContractError("Migration journal conflict; upgrade requires a new migration ID")
        for statement, table, columns in statements:
            spark.sql(statement).collect()
            if table:
                schema = spark.table(table).schema
                actual = [(field.name, field.dataType.simpleString(), field.nullable) for field in schema.fields]
                if actual != columns:
                    raise ContractError(f"Existing table schema differs: {table}")
        if not existing:
            spark.sql(
                f"INSERT INTO {journal} SELECT {literal(environment.project_id)}, {literal(key)}, "
                f"{literal(migration['source_sha256'])}, {literal(digest)}, 'APPLIED', {timestamp(now)}"
            ).collect()
        observed = rows(spark, f"SELECT * FROM {journal} WHERE {condition}")
        if len(observed) != 1 or observed[0]["source_sha256"] != migration["source_sha256"]:
            raise ContractError("Migration journal readback failed")
        outcomes.append({"id": key, "status": "APPLIED", "source_sha256": migration["source_sha256"],
                         "action": "verified" if existing else "created_and_verified"})
    return outcomes
