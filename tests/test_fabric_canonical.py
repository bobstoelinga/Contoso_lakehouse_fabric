from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest

from fabric_etl.canonical import scalar, spark_scalar
from fabric_etl.contracts import ContractError


@pytest.mark.parametrize("value,kind,expected", [
    (None, "STRING", None), (" a ", "STRING", "A"), ("", "STRING", ""),
    (True, "BOOLEAN", "TRUE"), (False, "BOOLEAN", "FALSE"),
    (9223372036854775807, "LONG", "9223372036854775807"), (-128, "BYTE", "-128"),
    (Decimal("-0.0000"), "DECIMAL(18,4)", "0.0000"),
    (Decimal("12.1"), "DECIMAL(18,4)", "12.1000"),
    (Decimal("99999999999999999999.999999999999999999"), "DECIMAL(38,18)",
     "99999999999999999999.999999999999999999"),
    (date(2026, 10, 10), "DATE", "2026-10-10"),
    (datetime(2026, 10, 25, 2, 30, 0, 123456, tzinfo=timezone(timedelta(hours=2))),
     "TIMESTAMP", "2026-10-25T00:30:00.123456Z"),
    (datetime(2026, 10, 25, 2, 30, 0, 123456, tzinfo=timezone(timedelta(hours=1))),
     "TIMESTAMP", "2026-10-25T01:30:00.123456Z"),
])
def test_python_canonical_vectors(value, kind, expected):
    assert scalar(value, kind) == expected


@pytest.mark.parametrize("value,kind", [
    (1, "BOOLEAN"), ("true", "BOOLEAN"), (True, "INT"), (128, "BYTE"),
    (9223372036854775808, "LONG"), ("12.1", "DECIMAL(18,4)"),
    (Decimal("1.12345"), "DECIMAL(18,4)"), (Decimal("1000"), "DECIMAL(3,0)"),
    (Decimal("NaN"), "DECIMAL(18,4)"), (float("inf"), "DOUBLE"),
    (b"test", "BINARY"), (datetime(2026, 10, 10), "TIMESTAMP"),
    (datetime(2026, 10, 10), "DATE"), ([1], "ARRAY<STRING>"),
])
def test_invalid_types_rounding_overflow_and_implicit_timezone_fail(value, kind):
    with pytest.raises(ContractError):
        scalar(value, kind)


def test_spark_scalar_requires_a_simple_column_and_rejects_float_hashes():
    assert spark_scalar("amount", "DECIMAL(18,4)") == "CAST(`amount` AS STRING)"
    with pytest.raises(ContractError):
        spark_scalar("amount); DROP TABLE x", "STRING")
    with pytest.raises(ContractError):
        spark_scalar("amount", "DOUBLE")
