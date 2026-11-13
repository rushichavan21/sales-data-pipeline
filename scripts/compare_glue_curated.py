"""Compare Athena table over AWS Glue curated output vs Local-Spark curated output."""

import os
from scripts.athena_runner import (
    execute_query,
    fetch_query_results,
    get_athena_client,
    print_table,
)

DATABASE = "sales_db"
BUCKET = os.getenv("S3_BUCKET_NAME", "sales-pipeline-rushi-2026-unique")


def main():
    client = get_athena_client()

    print("\n=======================================================")
    print("STEP 1: CREATE EXTERNAL TABLE OVER GLUE CURATED PARQUET")
    print("=======================================================")

    ddl = f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE}.sales_curated_glue (
        row_id STRING,
        order_id STRING,
        order_date DATE,
        ship_date DATE,
        ship_mode STRING,
        customer_id STRING,
        customer_name STRING,
        segment STRING,
        country STRING,
        city STRING,
        state STRING,
        postal_code STRING,
        region STRING,
        product_id STRING,
        category STRING,
        sub_category STRING,
        product_name STRING,
        sales DOUBLE,
        quantity INT,
        discount DOUBLE,
        profit DOUBLE,
        cost DOUBLE,
        profit_margin DOUBLE,
        ingestion_timestamp TIMESTAMP
    )
    PARTITIONED BY (
        order_year INT,
        order_month INT
    )
    STORED AS PARQUET
    LOCATION 's3://{BUCKET}/curated-glue/sales_curated/'
    TBLPROPERTIES (
        'parquet.compression' = 'SNAPPY'
    );
    """
    execute_query(client, ddl, database=DATABASE)

    print("\n[Executing] MSCK REPAIR TABLE on sales_curated_glue...")
    execute_query(client, f"MSCK REPAIR TABLE {DATABASE}.sales_curated_glue;", database=DATABASE)

    print("\n=======================================================")
    print("STEP 2: ROW COUNT COMPARISON")
    print("=======================================================")

    count_glue_id, _ = execute_query(client, f"SELECT COUNT(*) AS total_rows FROM {DATABASE}.sales_curated_glue;", database=DATABASE)
    count_glue = fetch_query_results(client, count_glue_id)[1][0]

    count_local_id, _ = execute_query(client, f"SELECT COUNT(*) AS total_rows FROM {DATABASE}.sales_curated;", database=DATABASE)
    count_local = fetch_query_results(client, count_local_id)[1][0]

    print(f"Row Count (Local Spark Curated):   {int(count_local):,}")
    print(f"Row Count (AWS Glue Curated):      {int(count_glue):,}")
    print(f"Difference:                        {int(count_glue) - int(count_local)}")

    print("\n=======================================================")
    print("STEP 3: ANNUAL SALES & PROFIT AGGREGATE COMPARISON")
    print("=======================================================")

    agg_sql_template = """
    SELECT
        order_year,
        COUNT(*) AS row_count,
        ROUND(SUM(sales), 2) AS total_sales,
        ROUND(SUM(profit), 2) AS total_profit
    FROM {DATABASE}.{table}
    GROUP BY order_year
    ORDER BY order_year;
    """

    print("\n--- Local PySpark Curated (Annual Aggregates) ---")
    q_local, _ = execute_query(client, agg_sql_template.format(DATABASE=DATABASE, table="sales_curated"), database=DATABASE)
    res_local = fetch_query_results(client, q_local)
    print_table(res_local[0], res_local[1:])

    print("\n--- AWS Glue Curated (Annual Aggregates) ---")
    q_glue, _ = execute_query(client, agg_sql_template.format(DATABASE=DATABASE, table="sales_curated_glue"), database=DATABASE)
    res_glue = fetch_query_results(client, q_glue)
    print_table(res_glue[0], res_glue[1:])

    print("\n=======================================================")
    print("STEP 4: CATEGORY BREAKDOWN COMPARISON")
    print("=======================================================")

    cat_sql_template = """
    SELECT
        category,
        COUNT(*) AS row_count,
        ROUND(SUM(sales), 2) AS total_sales,
        ROUND(SUM(profit), 2) AS total_profit
    FROM {DATABASE}.{table}
    GROUP BY category
    ORDER BY category;
    """

    print("\n--- Local PySpark Curated (Category Aggregates) ---")
    q_cat_local, _ = execute_query(client, cat_sql_template.format(DATABASE=DATABASE, table="sales_curated"), database=DATABASE)
    res_cat_local = fetch_query_results(client, q_cat_local)
    print_table(res_cat_local[0], res_cat_local[1:])

    print("\n--- AWS Glue Curated (Category Aggregates) ---")
    q_cat_glue, _ = execute_query(client, cat_sql_template.format(DATABASE=DATABASE, table="sales_curated_glue"), database=DATABASE)
    res_cat_glue = fetch_query_results(client, q_cat_glue)
    print_table(res_cat_glue[0], res_cat_glue[1:])


if __name__ == "__main__":
    main()
