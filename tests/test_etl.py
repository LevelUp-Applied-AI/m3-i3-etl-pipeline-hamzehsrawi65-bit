import pandas as pd
import pytest

from etl_pipeline import transform, validate


def test_transform_filters_cancelled():
    customers = pd.DataFrame({
        "customer_id": [1],
        "name": ["Hamza"],
        "city": ["Amman"],
        "registration_date": ["2026-01-01"]
    })

    products = pd.DataFrame({
        "product_id": [10],
        "name": ["Laptop"],
        "category": ["Electronics"],
        "unit_price": [1000]
    })

    orders = pd.DataFrame({
        "order_id": [100, 101],
        "customer_id": [1, 1],
        "order_date": ["2026-03-01", "2026-03-02"],
        "status": ["completed", "cancelled"]
    })

    order_items = pd.DataFrame({
        "item_id": [1, 2],
        "order_id": [100, 101],
        "product_id": [10, 10],
        "quantity": [1, 1]
    })

    data = {
        "customers": customers,
        "products": products,
        "orders": orders,
        "order_items": order_items
    }

    result = transform(data)

    assert len(result) == 1
    assert result.iloc[0]["total_orders"] == 1
    assert result.iloc[0]["total_revenue"] == 1000
    assert result.iloc[0]["customer_name"] == "Hamza"


def test_transform_filters_suspicious_quantity():
    customers = pd.DataFrame({
        "customer_id": [1],
        "name": ["Hamza"],
        "city": ["Amman"],
        "registration_date": ["2026-01-01"]
    })

    products = pd.DataFrame({
        "product_id": [10],
        "name": ["Laptop"],
        "category": ["Electronics"],
        "unit_price": [1000]
    })

    orders = pd.DataFrame({
        "order_id": [100],
        "customer_id": [1],
        "order_date": ["2026-03-01"],
        "status": ["completed"]
    })

    order_items = pd.DataFrame({
        "item_id": [1, 2],
        "order_id": [100, 100],
        "product_id": [10, 10],
        "quantity": [1, 150]
    })

    data = {
        "customers": customers,
        "products": products,
        "orders": orders,
        "order_items": order_items
    }

    result = transform(data)

    assert len(result) == 1
    assert result.iloc[0]["total_orders"] == 1
    assert result.iloc[0]["total_revenue"] == 1000


def test_validate_catches_nulls():
    df = pd.DataFrame({
        "customer_id": [None],
        "customer_name": ["Hamza"],
        "total_orders": [1],
        "total_revenue": [1000],
        "avg_order_value": [1000],
        "top_category": ["Electronics"]
    })

    with pytest.raises(ValueError):
        validate(df)