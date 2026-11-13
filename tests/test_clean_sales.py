"""Unit tests for PySpark data cleaning and enrichment logic."""

import datetime
from glue_jobs.clean_sales import (
    clean_and_standardize_data,
    enrich_sales_data,
    get_raw_schema,
)
from pyspark.sql import Row


def test_clean_and_standardize_dates_and_currency(spark):
    """Test date parsing across multiple formats and currency stripping."""
    schema = get_raw_schema()
    raw_data = [
        (
            "1", "CA-2024-1001", "2024-05-10", "2024-05-12", "Standard Class",
            "CUST-1", " Alice Smith ", "Consumer", "United States", "New York City",
            "New York", "10001", "East", "TEC-PH-101", "Technology",
            "Phones", "Smartphone", "$250.00", "2", "0.1", "50.0"
        ),
        (
            "2", "CA-2024-1002", "11/20/2023", "11/25/2023", "Second Class",
            "CUST-2", "Bob Jones", "Corporate", "United States", "Chicago",
            "Illinois", "60601", "Central", "OFF-BI-102", "Office Supplies",
            "Binders", "Binder", "15.50", "4", "0.0", "3.0"
        ),
        (
            "3", "CA-2024-1003", "15-08-2022", "18-08-2022", "First Class",
            "CUST-3", "Charlie", "Home Office", "United States", "Seattle",
            "Washington", "98101", "West", "FUR-CH-103", "Furniture",
            "Chairs", "Desk Chair", "120.00", "1", "0.2", "-10.0"
        ),
    ]

    raw_df = spark.createDataFrame(raw_data, schema=schema)
    cleaned_df = clean_and_standardize_data(raw_df)
    results = {row["order_id"]: row for row in cleaned_df.collect()}

    # Verify dates are parsed to datetime.date
    assert results["CA-2024-1001"]["order_date"] == datetime.date(2024, 5, 10)
    assert results["CA-2024-1002"]["order_date"] == datetime.date(2023, 11, 20)
    assert results["CA-2024-1003"]["order_date"] == datetime.date(2022, 8, 15)

    # Verify trimmed strings
    assert results["CA-2024-1001"]["customer_name"] == "Alice Smith"

    # Verify stripped dollar signs and typed numerics
    assert results["CA-2024-1001"]["sales"] == 250.0
    assert results["CA-2024-1001"]["quantity"] == 2
    assert results["CA-2024-1001"]["discount"] == 0.1
    assert results["CA-2024-1001"]["profit"] == 50.0


def test_deduplication(spark):
    """Test duplicate (order_id, product_id) lines are dropped."""
    schema = get_raw_schema()
    raw_data = [
        (
            "1", "CA-2024-2001", "2024-01-01", "2024-01-03", "Standard Class",
            "C-1", "Dave", "Consumer", "United States", "Austin", "Texas", "73301",
            "Central", "PROD-A", "Furniture", "Tables", "Table A", "100.0", "1", "0.0", "20.0"
        ),
        (
            "2", "CA-2024-2001", "2024-01-01", "2024-01-03", "Standard Class",
            "C-1", "Dave", "Consumer", "United States", "Austin", "Texas", "73301",
            "Central", "PROD-A", "Furniture", "Tables", "Table A", "100.0", "1", "0.0", "20.0"
        ),
    ]

    raw_df = spark.createDataFrame(raw_data, schema=schema)
    cleaned_df = clean_and_standardize_data(raw_df)
    assert cleaned_df.count() == 1


def test_null_imputation_and_filtering(spark):
    """Test null imputation on non-fatal fields and filtering on invalid records."""
    schema = get_raw_schema()
    raw_data = [
        # Missing customer name & postal code -> should be imputed
        (
            "1", "CA-2024-3001", "2024-01-01", "2024-01-03", "Standard Class",
            "C-1", None, None, "United States", "Miami", "Florida", "",
            "South", "PROD-1", "Technology", "Accessories", "Mouse", "25.0", "1", "0.0", "5.0"
        ),
        # Null order_id -> should be dropped
        (
            "2", None, "2024-01-01", "2024-01-03", "Standard Class",
            "C-2", "Eve", "Consumer", "United States", "Miami", "Florida", "33101",
            "South", "PROD-2", "Technology", "Accessories", "Mouse", "25.0", "1", "0.0", "5.0"
        ),
        # Zero quantity -> should be dropped
        (
            "3", "CA-2024-3003", "2024-01-01", "2024-01-03", "Standard Class",
            "C-3", "Frank", "Consumer", "United States", "Miami", "Florida", "33101",
            "South", "PROD-3", "Technology", "Accessories", "Mouse", "25.0", "0", "0.0", "5.0"
        ),
        # Negative sales -> should be dropped
        (
            "4", "CA-2024-3004", "2024-01-01", "2024-01-03", "Standard Class",
            "C-4", "Grace", "Consumer", "United States", "Miami", "Florida", "33101",
            "South", "PROD-4", "Technology", "Accessories", "Mouse", "-25.0", "1", "0.0", "5.0"
        ),
    ]

    raw_df = spark.createDataFrame(raw_data, schema=schema)
    cleaned_df = clean_and_standardize_data(raw_df)
    rows = cleaned_df.collect()

    assert len(rows) == 1
    retained_row = rows[0]
    assert retained_row["order_id"] == "CA-2024-3001"
    assert retained_row["customer_name"] == "Unknown Customer"
    assert retained_row["postal_code"] == "00000"
    assert retained_row["segment"] == "Consumer"


def test_enrich_sales_data(spark):
    """Test derivation of order_year, order_month, cost, and profit_margin."""
    schema = get_raw_schema()
    raw_data = [
        (
            "1", "CA-2024-4001", "2024-07-22", "2024-07-25", "Standard Class",
            "C-10", "Helen", "Corporate", "United States", "Denver", "Colorado", "80201",
            "West", "PROD-10", "Office Supplies", "Paper", "Paper Pack", "100.0", "2", "0.1", "20.0"
        )
    ]
    raw_df = spark.createDataFrame(raw_data, schema=schema)
    cleaned_df = clean_and_standardize_data(raw_df)
    enriched_df = enrich_sales_data(cleaned_df)
    row = enriched_df.collect()[0]

    # Partitions
    assert row["order_year"] == 2024
    assert row["order_month"] == 7

    # Cost = sales * (1 - discount) - profit = 100 * 0.9 - 20 = 70.0
    assert row["cost"] == 70.0

    # Profit margin = profit / sales = 20 / 100 = 0.20
    assert row["profit_margin"] == 0.2
    assert row["ingestion_timestamp"] is not None
