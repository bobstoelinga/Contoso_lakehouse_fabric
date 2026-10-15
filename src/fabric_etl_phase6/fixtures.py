"""Small explicit synthetic Sales scenarios, not original Contoso exports."""

from __future__ import annotations

from copy import deepcopy

from fabric_etl.contracts import ContractError


BASE = {
    "SALES.CUSTOMERS": {
        "customer_key": "P6-C1", "customer_name": "customer one", "email": None, "phone": "+31 123",
        "address_line1": "street 1", "city": "utrecht", "state_province": "ut",
        "postal_code": "1234AB", "country": "nl", "customer_segment": "retail",
        "customer_since": "2020-01-01", "is_deleted": "false",
    },
    "SALES.PRODUCTS": {
        "product_key": "P6-P1", "product_name": "product one", "product_category": "cat",
        "product_subcategory": "sub", "brand": "brand", "unit_cost": "8.0",
        "unit_price": "10.0", "is_discontinued": "false", "is_deleted": "false",
    },
    "SALES.EMPLOYEES": {
        "employee_key": "P6-E1", "first_name": "first", "last_name": "last",
        "job_title": "sales", "office_city": "utrecht", "hire_date": "2020-01-01", "is_deleted": "false",
    },
    "SALES.ORDERS": {
        "order_key": "P6-O1", "order_line_number": "1", "customer_key": "P6-C1", "product_key": "P6-P1",
        "employee_key": "P6-E1", "order_date": "2026-10-10", "ship_date": "2026-10-11",
        "delivery_date": "2026-10-11", "order_status": "delivered", "quantity": "2",
        "unit_price": "10.0", "discount_amount": "0", "net_amount": "20.0", "currency_code": "usd",
    },
    "SALES.RETURNS": {
        "return_key": "P6-R1", "order_key": "P6-O1", "order_line_number": "1", "product_key": "P6-P1",
        "employee_key": "P6-E1", "return_date": "2026-10-11", "return_status": "accepted",
        "return_reason_code": "damaged", "return_quantity": "1", "refund_amount": "10.0", "currency_code": "usd",
    },
}


def sales_fixture(number: int) -> dict[str, list[dict[str, str | None]]]:
    if type(number) is not int or number not in {1, 2, 3}:
        raise ContractError("Sales fixture number must be 1, 2 or 3")
    result = {source: [deepcopy(record)] for source, record in BASE.items()}
    if number >= 2:
        result["SALES.CUSTOMERS"][0]["customer_name"] = "customer changed"
        result["SALES.PRODUCTS"][0]["unit_price"] = "12.0"
    if number == 3:
        result["SALES.CUSTOMERS"][0]["is_deleted"] = "true"
        result["SALES.PRODUCTS"][0]["is_deleted"] = "true"
        result["SALES.EMPLOYEES"][0]["is_deleted"] = "true"
    return result
