"""Manifest-scoped Raw reads and retry-safe Bronze delivery commits."""

from __future__ import annotations

import re
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any

from fabric_etl.contracts import ContractError, canonical_json, fingerprint, identifier, required_text
from fabric_etl.delivery import Manifest


RAW_RECORD_COLUMNS = (
    "project_id", "source_system_id", "delivery_id", "delivery_sequence",
    "source_object_id", "source_file", "raw_record_id", "payload_json",
    "payload_sha256", "duplicate_ordinal", "batch_id", "release_id",
    "load_date_utc",
)
RAW_IDENTITY_FIELDS = (
    "delivery_id", "duplicate_ordinal", "file_sha256", "payload_sha256",
    "project_id", "source_file", "source_object_id", "source_system_id", "version",
)
RESERVED_COLUMNS = set(RAW_RECORD_COLUMNS) | {"_payload_json", "_payload_sha256", "_source_file"}
FORMAT_OPTIONS = {
    "JSON": {"multiLine", "encoding"},
    "CSV": {"header", "delimiter", "encoding", "quote", "escape"},
    "PARQUET": set(),
}


def validate_file_inventory(manifest: Manifest, actual_files: list[str]) -> None:
    """Require a platform listing to match the closed manifest exactly."""
    expected = {
        entry.path.casefold()
        for obj in manifest.objects
        for entry in obj.files
    }
    actual: set[str] = set()
    for path in actual_files:
        if not isinstance(path, str) or not path:
            raise ContractError("Invalid or duplicate file in delivery listing")
        if path == "_manifest.json":
            continue
        if path.casefold() in actual:
            raise ContractError("Invalid or duplicate file in delivery listing")
        actual.add(path.casefold())
    if actual != expected:
        raise ContractError(
            "Delivery file inventory differs from manifest "
            f"(missing={sorted(expected - actual)}, unexpected={sorted(actual - expected)})"
        )


def raw_record_identity(
    *, project_id: str, source_system_id: str, delivery_id: str,
    source_object_id: str, source_file: str, file_sha256: str,
    payload_sha256: str, duplicate_ordinal: int,
) -> str:
    identifier(project_id)
    identifier(source_system_id)
    required_text(delivery_id, "delivery_id")
    if not isinstance(source_object_id, str) or not re.fullmatch(
        r"[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*", source_object_id,
    ):
        raise ContractError("Invalid source object identity")
    if (
        not isinstance(source_file, str)
        or not re.fullmatch(r"[A-Za-z0-9_.-]+(?:/[A-Za-z0-9_.-]+)*", source_file)
        or source_file.startswith("/")
        or any(part in {"", ".", ".."} for part in source_file.split("/"))
        or not re.fullmatch(r"[0-9a-f]{64}", file_sha256)
        or not re.fullmatch(r"[0-9a-f]{64}", payload_sha256)
        or type(duplicate_ordinal) is not int or duplicate_ordinal < 1
        or not re.fullmatch(r"[A-Za-z0-9_.|:-]+", delivery_id)
    ):
        raise ContractError("Invalid raw-record identity input")
    identity_payload = {
        "delivery_id": delivery_id,
        "duplicate_ordinal": duplicate_ordinal,
        "file_sha256": file_sha256,
        "payload_sha256": payload_sha256,
        "project_id": project_id,
        "source_file": source_file,
        "source_object_id": source_object_id,
        "source_system_id": source_system_id,
        "version": "fabric-raw-record-v1",
    }
    return fingerprint(identity_payload)


def spark_schema(columns: list[dict[str, Any]]) -> Any:
    """Build an explicit PySpark schema from declared project columns."""
    from pyspark.sql import types

    mapping = {
        "STRING": types.StringType,
        "BOOLEAN": types.BooleanType,
        "BYTE": types.ByteType,
        "SHORT": types.ShortType,
        "INT": types.IntegerType,
        "LONG": types.LongType,
        "DATE": types.DateType,
        "TIMESTAMP": types.TimestampType,
        "DOUBLE": types.DoubleType,
        "FLOAT": types.FloatType,
        "BINARY": types.BinaryType,
    }
    fields = []
    seen: set[str] = set()
    for column in columns:
        name = identifier(column["name"])
        kind = column["data_type"]
        if name in seen or name in RESERVED_COLUMNS:
            raise ContractError(f"Duplicate or reserved source column: {name}")
        seen.add(name)
        if kind.startswith("DECIMAL(") and kind.endswith(")"):
            precision, scale = (int(part) for part in kind[8:-1].split(","))
            data_type = types.DecimalType(precision, scale)
        elif kind in mapping:
            data_type = mapping[kind]()
        else:
            raise ContractError(f"Unsupported Spark source datatype: {kind}")
        nullable = column["nullable"]
        if type(nullable) is not bool:
            raise ContractError(f"Invalid nullable declaration for {name}")
        fields.append(types.StructField(name, data_type, nullable))
    if not fields:
        raise ContractError("Source schema must contain columns")
    return types.StructType(fields)


def read_manifest_records(
    spark: Any, *, root_path: str, manifest: Manifest, project_id: str,
    source_formats: dict[str, str], schemas: dict[str, Any],
    batch_id: str, release_id: str, load_date_utc: str,
    options: dict[str, dict[str, str | bool]] | None = None,
) -> Any:
    """Read every declared object/file with explicit schema and stable row IDs."""
    from pyspark.sql import Window, functions as F

    object_ids = {obj.source_object_id for obj in manifest.objects}
    if set(source_formats) != object_ids or set(schemas) != object_ids:
        raise ContractError("Reader format/schema scope must exactly match manifest objects")
    if options is not None and set(options) - object_ids:
        raise ContractError("Reader options contain an object outside manifest scope")
    if not all(isinstance(value, str) and value for value in (root_path, batch_id, release_id, load_date_utc)):
        raise ContractError("Raw root and persisted batch context are required")
    identifier(project_id)
    try:
        parsed_load_date = datetime.fromisoformat(load_date_utc.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ContractError("load_date_utc must be an ISO-8601 UTC timestamp") from exc
    if (
        parsed_load_date.tzinfo is None
        or parsed_load_date.utcoffset() != timezone.utc.utcoffset(parsed_load_date)
    ):
        raise ContractError("load_date_utc must identify UTC explicitly")

    frames = []
    for obj in manifest.objects:
        file_format = source_formats[obj.source_object_id]
        if file_format not in FORMAT_OPTIONS:
            raise ContractError(f"Unsupported Raw file format: {file_format}")
        object_options = {} if options is None else options.get(obj.source_object_id, {})
        if set(object_options) - FORMAT_OPTIONS[file_format]:
            raise ContractError(f"Unsupported {file_format} reader option")
        schema = schemas[obj.source_object_id]
        source_columns = [field.name for field in schema.fields]
        if not source_columns or RESERVED_COLUMNS.intersection(source_columns):
            raise ContractError("Source schema is empty or collides with Raw metadata")

        file_frames = []
        for entry in obj.files:
            path = root_path.rstrip("/") + "/" + entry.path
            reader = spark.read.format(file_format.lower())
            if file_format in {"JSON", "CSV"}:
                reader = reader.option("mode", "FAILFAST")
            if object_options:
                reader = reader.options(**object_options)
            inferred = reader.load(path)
            observed_names = [field.name for field in inferred.schema.fields]
            declared_names = [field.name for field in schema.fields]
            if (
                (file_format == "JSON" and set(observed_names) != set(declared_names))
                or (file_format != "JSON" and observed_names != declared_names)
            ):
                raise ContractError(f"Raw schema differs from declared schema: {obj.source_object_id}")
            if file_format == "PARQUET":
                observed_types = [(field.name, field.dataType.simpleString()) for field in inferred.schema.fields]
                declared_types = [(field.name, field.dataType.simpleString()) for field in schema.fields]
                if observed_types != declared_types:
                    raise ContractError(f"Parquet datatypes differ from declared schema: {obj.source_object_id}")
            frame = spark.read.format(file_format.lower()).schema(schema)
            if file_format in {"JSON", "CSV"}:
                frame = frame.option("mode", "FAILFAST")
            if object_options:
                frame = frame.options(**object_options)
            frame = frame.load(path)
            payload = F.to_json(
                F.struct(*[F.col(name).alias(name) for name in sorted(source_columns)]),
                options={"ignoreNullFields": "false"},
            )
            identity_columns = {
                "delivery_id": F.lit(manifest.delivery_id),
                "duplicate_ordinal": F.col("duplicate_ordinal"),
                "file_sha256": F.lit(entry.sha256),
                "payload_sha256": F.col("_payload_sha256"),
                "project_id": F.lit(project_id),
                "source_file": F.col("_source_file"),
                "source_object_id": F.lit(obj.source_object_id),
                "source_system_id": F.lit(manifest.source_system_id),
                "version": F.lit("fabric-raw-record-v1"),
            }
            frame = (
                frame
                .withColumn("_source_file", F.lit(entry.path))
                .withColumn("_payload_json", payload)
                .withColumn("_payload_sha256", F.sha2(F.col("_payload_json"), 256))
                .withColumn(
                    "duplicate_ordinal",
                    F.row_number().over(
                        Window.partitionBy("_source_file", "_payload_json").orderBy(F.lit(1))
                    ).cast("long"),
                )
                .withColumn(
                    "raw_record_id",
                    F.sha2(
                        F.to_json(F.struct(*[
                            identity_columns[name].alias(name)
                            for name in RAW_IDENTITY_FIELDS
                        ])),
                        256,
                    ),
                )
                .select(
                    F.lit(project_id).alias("project_id"),
                    F.lit(manifest.source_system_id).alias("source_system_id"),
                    F.lit(manifest.delivery_id).alias("delivery_id"),
                    F.lit(manifest.delivery_sequence).cast("long").alias("delivery_sequence"),
                    F.lit(obj.source_object_id).alias("source_object_id"),
                    F.col("_source_file").alias("source_file"),
                    F.col("raw_record_id"),
                    F.col("_payload_json").alias("payload_json"),
                    F.col("_payload_sha256").alias("payload_sha256"),
                    F.col("duplicate_ordinal"),
                    F.lit(batch_id).alias("batch_id"),
                    F.lit(release_id).alias("release_id"),
                    F.lit(load_date_utc).alias("load_date_utc"),
                )
            )
            file_frames.append(frame)
        if not file_frames:
            if not obj.empty_snapshot:
                raise ContractError(f"Manifest has no files for non-empty object {obj.source_object_id}")
            empty = spark.createDataFrame([], schema)
            payload = F.to_json(
                F.struct(*[F.col(name).alias(name) for name in sorted(source_columns)]),
                options={"ignoreNullFields": "false"},
            )
            file_frames.append(
                empty.withColumn("_source_file", F.lit(""))
                .withColumn("_payload_json", payload)
                .withColumn("_payload_sha256", F.sha2(F.col("_payload_json"), 256))
                .withColumn("duplicate_ordinal", F.lit(None).cast("long"))
                .withColumn("raw_record_id", F.lit(None).cast("string"))
                .select(
                    F.lit(project_id).alias("project_id"),
                    F.lit(manifest.source_system_id).alias("source_system_id"),
                    F.lit(manifest.delivery_id).alias("delivery_id"),
                    F.lit(manifest.delivery_sequence).cast("long").alias("delivery_sequence"),
                    F.lit(obj.source_object_id).alias("source_object_id"),
                    F.col("_source_file").alias("source_file"),
                    F.col("raw_record_id"),
                    F.col("_payload_json").alias("payload_json"),
                    F.col("_payload_sha256").alias("payload_sha256"),
                    F.col("duplicate_ordinal"),
                    F.lit(batch_id).alias("batch_id"),
                    F.lit(release_id).alias("release_id"),
                    F.lit(load_date_utc).alias("load_date_utc"),
                )
            )
        frames.extend(file_frames)

    result = frames[0]
    for frame in frames[1:]:
        result = result.unionByName(frame)
    return result


def _scope_rows(frame: Any, project_id: str, source_system_id: str, delivery_id: str) -> Any:
    from pyspark.sql import functions as F

    return frame.where(
        (F.col("project_id") == project_id)
        & (F.col("source_system_id") == source_system_id)
        & (F.col("delivery_id") == delivery_id)
    ).select(*RAW_RECORD_COLUMNS)


def _reconcile_frames(expected: Any, actual: Any) -> Any:
    from pyspark.sql import functions as F

    for frame, label in ((expected, "expected"), (actual, "stored")):
        if frame.groupBy("raw_record_id").count().where(F.col("count") != 1).limit(1).count():
            raise ContractError(f"Duplicate raw_record_id in {label} Bronze records")
    extra = actual.join(expected.select("raw_record_id"), "raw_record_id", "left_anti")
    if extra.limit(1).count():
        raise ContractError("Stored Bronze delivery contains records absent from manifest input")
    joined = actual.alias("stored").join(expected.alias("expected"), "raw_record_id")
    comparisons = [
        ~F.col(f"stored.`{name}`").eqNullSafe(F.col(f"expected.`{name}`"))
        for name in RAW_RECORD_COLUMNS if name != "raw_record_id"
    ]
    conflict_condition = comparisons[0]
    for condition in comparisons[1:]:
        conflict_condition = conflict_condition | condition
    conflicts = joined.where(conflict_condition)
    if conflicts.limit(1).count():
        raise ContractError("Stored Bronze record differs from deterministic Raw input or retry context")
    return expected.join(actual.select("raw_record_id"), "raw_record_id", "left_anti")


def _output_fingerprint(frame: Any) -> tuple[int, str]:
    from pyspark.sql import functions as F

    count = frame.count()
    buckets = (
        frame.withColumn("_row_hash", F.sha2(
            F.to_json(F.struct(*[F.col(name) for name in RAW_RECORD_COLUMNS]),
                      options={"ignoreNullFields": "false"}), 256,
        ))
        .withColumn("_bucket", F.substring("raw_record_id", 1, 2))
        .groupBy("_bucket")
        .agg(
            F.count(F.lit(1)).alias("count"),
            F.sha2(F.concat_ws("", F.sort_array(F.collect_list("_row_hash"))), 256).alias("sha256"),
        )
        .orderBy("_bucket")
        .collect()
    )
    result = [
        {"bucket": row["_bucket"], "count": row["count"], "sha256": row["sha256"]}
        for row in buckets
    ]
    return count, fingerprint(result)


def write_bronze_delivery(
    spark: Any, records: Any, *, target_table: str, project_id: str,
    source_system_id: str, delivery_id: str, control: Any, owner: Any,
) -> dict[str, Any]:
    """Append only missing IDs, fail on conflicts, then reconcile exact readback."""
    match = re.fullmatch(
        r"`[A-Za-z_][A-Za-z0-9_]*`\.`[A-Za-z_][A-Za-z0-9_]*`\.`bronze`\.`([A-Za-z_][A-Za-z0-9_]*)`",
        target_table if isinstance(target_table, str) else "",
    )
    if match is None or target_table != control.environment.spark_table(
        "processing", "bronze", match[1],
    ):
        raise ContractError("Bronze target must be a bound Fabric table identifier")
    if [field.name for field in records.schema.fields] != list(RAW_RECORD_COLUMNS):
        raise ContractError("Bronze input columns differ from the registered raw-record contract")
    control.assert_owner(owner)
    columns = ", ".join(f"`{name}` STRING NOT NULL" for name in RAW_RECORD_COLUMNS)
    columns = columns.replace("`delivery_sequence` STRING", "`delivery_sequence` BIGINT")
    columns = columns.replace("`duplicate_ordinal` STRING", "`duplicate_ordinal` BIGINT")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {target_table} ({columns}) USING DELTA").collect()
    actual_schema = [(field.name, field.dataType.simpleString()) for field in spark.table(target_table).schema.fields]
    expected_schema = [
        (name, "bigint" if name in {"delivery_sequence", "duplicate_ordinal"} else "string")
        for name in RAW_RECORD_COLUMNS
    ]
    if actual_schema != expected_schema:
        raise ContractError("Existing Bronze target schema differs from the registered contract")
    expected = _scope_rows(records, project_id, source_system_id, delivery_id)
    table = spark.table(target_table)
    actual = _scope_rows(table, project_id, source_system_id, delivery_id)
    missing = _reconcile_frames(expected, actual)
    missing_count = missing.count()
    if missing_count:
        control.assert_owner(owner)
        missing_records = records.join(missing.select("raw_record_id"), "raw_record_id")
        missing_records.write.format("delta").mode("append").saveAsTable(target_table)
    control.assert_owner(owner)
    readback = _scope_rows(spark.table(target_table), project_id, source_system_id, delivery_id)
    still_missing = _reconcile_frames(expected, readback)
    if still_missing.limit(1).count():
        raise ContractError("Bronze readback is incomplete after Delta append")
    count, output_hash = _output_fingerprint(readback)
    return {
        "status": "SUCCEEDED",
        "record_count": count,
        "inserted_count": missing_count,
        "retry_count": count - missing_count,
        "output_fingerprint": output_hash,
        "target_table": target_table,
    }


def record_bronze_commit(
    control: Any, owner: Any, *, batch_id: str, source_system_id: str,
    delivery_id: str, manifest: Manifest, outputs: dict[str, Any], now: datetime,
) -> dict[str, str]:
    """Persist an idempotent COMMITTED marker after target readback succeeds."""
    from fabric_etl.control import literal, rows, timestamp

    control.assert_owner(owner)
    table = control.table("commit_manifest")
    input_fingerprint = fingerprint(asdict(manifest))
    outputs_json = canonical_json(outputs).decode("utf-8")
    commit_id = fingerprint([
        control.project, batch_id, source_system_id, delivery_id,
        input_fingerprint, outputs_json,
    ])
    existing = rows(control.spark, f"SELECT * FROM {table} WHERE commit_id = {literal(commit_id)}")
    if existing:
        if (
            len(existing) != 1
            or existing[0]["batch_id"] != batch_id
            or existing[0]["input_fingerprint"] != input_fingerprint
            or existing[0]["outputs_json"] != outputs_json
            or existing[0]["status"] != "COMMITTED"
        ):
            raise ContractError("Conflicting Bronze commit manifest")
        return {"commit_id": commit_id, "status": "VERIFIED"}
    control.spark.sql(
        f"INSERT INTO {table} SELECT {literal(commit_id)}, {literal(batch_id)}, "
        f"'BRONZE', {literal(source_system_id + '.' + delivery_id)}, {owner.generation}, "
        f"{literal(input_fingerprint)}, {literal(outputs_json)}, 'COMMITTED', {timestamp(now)}"
    ).collect()
    control.assert_owner(owner)
    observed = rows(control.spark, f"SELECT * FROM {table} WHERE commit_id = {literal(commit_id)}")
    if len(observed) != 1 or observed[0]["outputs_json"] != outputs_json:
        raise ContractError("Bronze commit manifest readback differs")
    return {"commit_id": commit_id, "status": "COMMITTED"}
