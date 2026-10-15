"""Independent expected rows for the explicit three-delivery Sales proof."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from fabric_etl.contracts import ContractError, hash_key


def expected_vault_rows(number: int) -> dict[str, dict[str, Any]]:
    if type(number) is not int or number not in {1, 2, 3}:
        raise ContractError("Expected Vault scenario must be 1, 2 or 3")
    customer, product, employee, order, returned = (
        hash_key("SALES", [key]) for key in ("P6-C1", "P6-P1", "P6-E1", "P6-O1", "P6-R1")
    )
    line = hash_key("SALES", ["P6-O1", "1", "P6-P1"])
    return_line = hash_key("SALES", ["P6-R1", "P6-O1", "1", "P6-P1"])
    deleted = number == 3
    return {
        "HUB_CUSTOMER": {"customer_key": "P6-C1", "hk_customer": customer},
        "HUB_PRODUCT": {"product_key": "P6-P1", "hk_product": product},
        "HUB_EMPLOYEE": {"employee_key": "P6-E1", "hk_employee": employee},
        "HUB_ORDER": {"order_key": "P6-O1", "hk_order": order},
        "HUB_RETURN": {"return_key": "P6-R1", "hk_return": returned},
        "LNK_ORDER_CUSTOMER": {
            "hk_order": order, "hk_customer": customer, "order_key": "P6-O1", "customer_key": "P6-C1",
            "hk_order_customer": hash_key("SALES", ["P6-O1", "P6-C1"]),
        },
        "LNK_ORDER_PRODUCT": {
            "hk_order": order, "hk_product": product, "order_line_number": 1,
            "order_key": "P6-O1", "product_key": "P6-P1", "hk_order_product": line,
        },
        "LNK_RETURN_ORDER_PRODUCT": {
            "hk_return": returned, "hk_order": order, "hk_product": product, "order_line_number": 1,
            "return_key": "P6-R1", "order_key": "P6-O1", "product_key": "P6-P1",
            "hk_return_order_product": return_line,
        },
        "LNK_ORDER_EMPLOYEE": {
            "hk_order": order, "hk_employee": employee, "order_key": "P6-O1", "employee_key": "P6-E1",
            "hk_order_employee": hash_key("SALES", ["P6-O1", "P6-E1"]),
        },
        "LNK_RETURN_EMPLOYEE": {
            "hk_return": returned, "hk_employee": employee, "return_key": "P6-R1", "employee_key": "P6-E1",
            "hk_return_employee": hash_key("SALES", ["P6-R1", "P6-E1"]),
        },
        "SAT_CUSTOMER": {
            "customer_name": "Customer One" if number == 1 else "Customer Changed",
            "email": None, "phone": "+31123", "address_line1": "street 1", "city": "Utrecht",
            "state_province": "UT", "postal_code": "1234AB", "country_code": "NL",
            "customer_segment": "RETAIL", "customer_since": date(2020, 1, 1),
            "is_deleted": deleted, "hk_customer": customer,
        },
        "SAT_PRODUCT": {
            "product_name": "product one", "product_category": "CAT", "product_subcategory": "SUB",
            "brand": "brand", "unit_cost": Decimal("8.0000"),
            "unit_price": Decimal("10.0000") if number == 1 else Decimal("12.0000"),
            "is_discontinued": False, "is_deleted": deleted, "hk_product": product,
        },
        "SAT_EMPLOYEE": {
            "first_name": "First", "last_name": "Last", "job_title": "sales", "office_city": "Utrecht",
            "hire_date": date(2020, 1, 1), "is_deleted": deleted, "hk_employee": employee,
        },
        "SAT_ORDER": {
            "order_status": "DELIVERED", "order_date": date(2026, 10, 10),
            "currency_code": "USD", "hk_order": order,
        },
        "SAT_ORDER_LINE": {
            "quantity": 2, "unit_price": Decimal("10.0000"), "discount_amount": Decimal("0.0000"),
            "net_amount": Decimal("20.0000"), "ship_date": date(2026, 10, 11),
            "delivery_date": date(2026, 10, 11), "hk_order_product": line,
        },
        "SAT_RETURN": {
            "return_date": date(2026, 10, 11), "return_status": "ACCEPTED", "return_reason_code": "DAMAGED",
            "return_quantity": 1, "refund_amount": Decimal("10.0000"), "currency_code": "USD",
            "hk_return_order_product": return_line,
        },
        "SAT_CUSTOMER_BV": {
            "full_address": "street 1, Utrecht, UT, 1234AB, NL", "customer_type": "B2C",
            "tenure_band": "LOYAL", "is_contactable": False, "hk_customer": customer,
        },
        "SAT_ORDER_LINE_BV": {
            "gross_amount": Decimal("20.0000"), "net_amount_calc": Decimal("20.0000"),
            "discount_rate": Decimal("0.000000"), "lead_time_days": 1,
            "is_cancelled": False, "hk_order_product": line,
        },
    }


def expected_event_count(entity_id: str, number: int) -> int:
    expected = expected_vault_rows(number)
    if entity_id not in expected:
        raise ContractError("No independent event-count expectation for this entity")
    if number == 1:
        return 1
    return int(entity_id in {"SAT_CUSTOMER", "SAT_PRODUCT"} or (
        number == 3 and entity_id == "SAT_EMPLOYEE"
    ))


def expected_gold_rows(number: int, load_dates: list[datetime]) -> dict[str, list[dict[str, Any]]]:
    expected_vault_rows(number)
    if len(load_dates) != number or any(
        value.tzinfo is None or value.utcoffset() != timedelta(0) for value in load_dates
    ):
        raise ContractError("Expected Gold rows require the exact persisted UTC delivery timeline")
    if any(later <= earlier for earlier, later in zip(load_dates, load_dates[1:])):
        raise ContractError("Expected Gold load dates must be strictly increasing")
    times = [value.astimezone(timezone.utc).replace(tzinfo=None) for value in load_dates]
    infinity = datetime(9999, 12, 31, 23, 59, 59)
    result: dict[str, list[dict[str, Any]]] = {}
    for dimension, satellite, hub, key in (
        ("CUSTOMER", "SAT_CUSTOMER", "HUB_CUSTOMER", "customer"),
        ("PRODUCT", "SAT_PRODUCT", "HUB_PRODUCT", "product"),
        ("EMPLOYEE", "SAT_EMPLOYEE", "HUB_EMPLOYEE", "employee"),
    ):
        changes = [index for index in range(1, number + 1) if expected_event_count(satellite, index)]
        historical = []
        for position, index in enumerate(changes):
            expected = expected_vault_rows(index)
            row = {
                **expected[satellite], key + "_hk": expected[hub]["hk_" + key],
                key + "_key": expected[hub][key + "_key"],
            }
            del row["hk_" + key]
            if dimension == "CUSTOMER":
                bv = dict(expected["SAT_CUSTOMER_BV"])
                del bv["hk_customer"]
                row.update(bv)
                del row["address_line1"]
            if dimension == "PRODUCT":
                row["unit_margin"] = row["unit_price"] - row["unit_cost"]
            row.update(
                valid_from=times[index - 1],
                valid_to=times[changes[position + 1] - 1] if position + 1 < len(changes) else infinity,
                is_current=position + 1 == len(changes), record_source="SALES",
            )
            historical.append(row)
        result["GH_DIM_" + dimension] = historical
        latest = historical[-1]
        current = []
        if not latest["is_deleted"]:
            row = {column: value for column, value in latest.items()
                   if column not in {"valid_from", "valid_to", "is_current", "record_source", "is_deleted"}}
            row["last_changed_at"] = latest["valid_from"]
            current.append(row)
        result["GC_DIM_" + dimension] = current
    expected = expected_vault_rows(1)
    sales = {
        "sales_line_hk": expected["LNK_ORDER_PRODUCT"]["hk_order_product"],
        "order_hk": expected["HUB_ORDER"]["hk_order"], "product_hk": expected["HUB_PRODUCT"]["hk_product"],
        "customer_hk": expected["HUB_CUSTOMER"]["hk_customer"],
        "employee_hk": expected["HUB_EMPLOYEE"]["hk_employee"], "order_key": "P6-O1",
        "order_line_number": 1, "order_date": date(2026, 10, 10), "order_date_key": 20261010,
        "order_status": "DELIVERED", "currency_code": "USD", "quantity": 2,
        "unit_price": Decimal("10.0000"), "discount_amount": Decimal("0.0000"),
        "gross_amount": Decimal("20.0000"), "net_amount": Decimal("20.0000"),
        "discount_rate": Decimal("0.000000"), "lead_time_days": 1, "is_cancelled": False,
        "ship_date": date(2026, 10, 11), "delivery_date": date(2026, 10, 11),
        "ship_date_key": 20261011, "delivery_date_key": 20261011,
    }
    returned = {
        "return_line_hk": expected["LNK_RETURN_ORDER_PRODUCT"]["hk_return_order_product"],
        "return_hk": expected["HUB_RETURN"]["hk_return"], "order_hk": sales["order_hk"],
        "product_hk": sales["product_hk"], "customer_hk": sales["customer_hk"],
        "employee_hk": sales["employee_hk"], "return_key": "P6-R1", "order_key": "P6-O1",
        "order_line_number": 1, "return_date": date(2026, 10, 11), "return_date_key": 20261011,
        "return_status": "ACCEPTED", "return_reason_code": "DAMAGED", "return_quantity": 1,
        "refund_amount": Decimal("10.0000"), "currency_code": "USD",
    }
    for name, row in (("SALES", sales), ("RETURNS", returned)):
        result["GC_FCT_" + name] = [dict(row)]
        result["GH_FCT_" + name] = [{
            **row, "valid_from": times[0], "valid_to": infinity, "is_current": True, "record_source": "SALES",
        }]
    calendar = []
    day = date(2020, 1, 1)
    months = ("January", "February", "March", "April", "May", "June", "July", "August",
              "September", "October", "November", "December")
    days = ("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")
    while day <= date(2030, 12, 31):
        calendar.append({
            "date_key": day.year * 10000 + day.month * 100 + day.day, "calendar_date": day,
            "calendar_year": day.year, "calendar_quarter": (day.month - 1) // 3 + 1,
            "month_number": day.month, "month_name": months[day.month - 1],
            "week_number": day.isocalendar().week, "day_of_month": day.day,
            "day_of_week": (day.weekday() + 1) % 7 + 1, "day_name": days[day.weekday()],
            "is_weekend": day.weekday() >= 5,
        })
        day += timedelta(days=1)
    result["GC_DIM_DATE"] = calendar
    return result
