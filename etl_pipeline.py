from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine


def extract(engine):
    customers = pd.read_sql("SELECT * FROM customers", engine)
    products = pd.read_sql("SELECT * FROM products", engine)
    orders = pd.read_sql("SELECT * FROM orders", engine)
    order_items = pd.read_sql("SELECT * FROM order_items", engine)

    return {
        "customers": customers,
        "products": products,
        "orders": orders,
        "order_items": order_items
    }


def transform(data_dict):
    customers = data_dict["customers"].copy()
    products = data_dict["products"].copy()
    orders = data_dict["orders"].copy()
    order_items = data_dict["order_items"].copy()

    # Filter out cancelled orders
    orders = orders[orders["status"] != "cancelled"]

    # Filter out suspicious quantities
    order_items = order_items[order_items["quantity"] <= 100]

    # Join all tables
    merged = orders.merge(order_items, on="order_id", how="inner")
    merged = merged.merge(products, on="product_id", how="inner")
    merged = merged.merge(customers, on="customer_id", how="inner")

    # Rename duplicated name columns after merge
    merged = merged.rename(columns={
        "name_x": "product_name",
        "name_y": "customer_name"
    })

    # Compute line total
    merged["line_total"] = merged["quantity"] * merged["unit_price"]

    # Customer-level summary
    customer_summary = (
        merged.groupby(["customer_id", "customer_name"], as_index=False)
        .agg(
            total_orders=("order_id", "nunique"),
            total_revenue=("line_total", "sum")
        )
    )

    # Average order value
    customer_summary["avg_order_value"] = (
        customer_summary["total_revenue"] / customer_summary["total_orders"]
    )

    # Top category per customer based on revenue
    category_revenue = (
        merged.groupby(["customer_id", "category"], as_index=False)
        .agg(category_revenue=("line_total", "sum"))
    )

    top_category = (
        category_revenue.sort_values(
            ["customer_id", "category_revenue"],
            ascending=[True, False]
        )
        .drop_duplicates(subset=["customer_id"])
        .rename(columns={"category": "top_category"})
        [["customer_id", "top_category"]]
    )

    # Merge top category into customer summary
    final_df = customer_summary.merge(top_category, on="customer_id", how="left")

    # Reorder columns
    final_df = final_df[
        [
            "customer_id",
            "customer_name",
            "total_orders",
            "total_revenue",
            "avg_order_value",
            "top_category"
        ]
    ]

    return final_df


def validate(df):
    checks = {
        "no_null_customer_id": df["customer_id"].notna().all(),
        "no_null_customer_name": df["customer_name"].notna().all(),
        "positive_total_revenue": (df["total_revenue"] > 0).all(),
        "unique_customer_id": ~df["customer_id"].duplicated().any(),
        "positive_total_orders": (df["total_orders"] > 0).all(),
    }

    for check_name, passed in checks.items():
        print(f"{check_name}: {'PASS' if passed else 'FAIL'}")

    if not all(checks.values()):
        raise ValueError("Validation failed: one or more critical checks did not pass.")

    return checks


def load(df, engine, csv_path):
    csv_path = Path(csv_path)
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    df.to_sql("customer_analytics", engine, if_exists="replace", index=False)
    df.to_csv(csv_path, index=False)

    print(f"Loaded {len(df)} rows to customer_analytics table")
    print(f"Saved CSV to {csv_path}")


def main():
    database_url = "postgresql+psycopg://postgres:postgres@localhost:5432/amman_market"
    engine = create_engine(database_url)

    print("Starting ETL pipeline...")

    data = extract(engine)
    print("Extract stage complete:")
    for table_name, df in data.items():
        print(f"{table_name}: {len(df)} rows")

    transformed_df = transform(data)
    print(f"Transform stage complete: {len(transformed_df)} rows")

    validate(transformed_df)
    print("Validation stage complete.")

    load(transformed_df, engine, "output/customer_analytics.csv")
    print("Load stage complete.")
    print("ETL pipeline finished successfully.")


if __name__ == "__main__":
    main()