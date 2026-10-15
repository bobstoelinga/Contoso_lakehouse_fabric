"""Hash scalar canonicalization; never round or guess a timezone."""

from __future__ import annotations

import re
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation, localcontext

from fabric_etl.contracts import ContractError, identifier
from fabric_etl.project import data_type


def scalar(value: object, kind: str) -> str | None:
    data_type(kind)
    if kind in {"FLOAT", "DOUBLE", "BINARY"}:
        raise ContractError("Business hash requires an explicit scalar projection")
    if value is None:
        return None
    if kind == "STRING":
        if not isinstance(value, str):
            raise ContractError("STRING hash input has an incompatible value")
        return value.strip().upper()
    if kind == "BOOLEAN":
        if type(value) is not bool:
            raise ContractError("BOOLEAN hash input must be a bool")
        return "TRUE" if value else "FALSE"
    if kind in {"BYTE", "SHORT", "INT", "LONG"}:
        bits = {"BYTE": 8, "SHORT": 16, "INT": 32, "LONG": 64}[kind]
        if type(value) is not int or not -(2 ** (bits - 1)) <= value < 2 ** (bits - 1):
            raise ContractError("Integer hash value outside declared type")
        return str(value)
    decimal = re.fullmatch(r"DECIMAL\((\d+),(\d+)\)", kind)
    if decimal:
        if not isinstance(value, Decimal) or not value.is_finite():
            raise ContractError("Decimal hash input must be a finite Decimal")
        precision, scale = int(decimal[1]), int(decimal[2])
        with localcontext() as context:
            context.prec = 80
            try:
                fixed = value.quantize(Decimal(1).scaleb(-scale))
            except InvalidOperation as exc:
                raise ContractError("Decimal cannot be represented") from exc
            if fixed != value or abs(value) >= Decimal(10) ** (precision - scale):
                raise ContractError("Decimal rounding or overflow is not permitted")
            if fixed == 0:
                fixed = abs(fixed)
            return format(fixed, f".{scale}f")
    if kind == "DATE":
        if type(value) is not date:
            raise ContractError("DATE hash input must be a date")
        return value.isoformat()
    if kind == "TIMESTAMP":
        if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
            raise ContractError("TIMESTAMP hash input requires an explicit timezone")
        return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    raise ContractError("Unsupported hash scalar")


def spark_scalar(column: str, kind: str) -> str:
    column = f"`{identifier(column)}`"
    data_type(kind)
    if kind in {"FLOAT", "DOUBLE", "BINARY"}:
        raise ContractError("Business hash requires an explicit scalar projection")
    if kind == "STRING":
        return f"upper(trim({column}))"
    if kind == "BOOLEAN":
        return f"upper(CAST({column} AS STRING))"
    if kind in {"BYTE", "SHORT", "INT", "LONG"} or kind.startswith("DECIMAL("):
        return f"CAST({column} AS STRING)"
    if kind == "DATE":
        return f"date_format({column}, 'yyyy-MM-dd')"
    if kind == "TIMESTAMP":
        # The caller must establish spark.sql.session.timeZone = UTC before evaluation.
        return f"date_format({column}, \"yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'\")"
    raise ContractError("Unsupported Spark hash scalar")
