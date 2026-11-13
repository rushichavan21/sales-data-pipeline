"""End-to-end Athena execution, query results display, and CSV vs Parquet benchmark."""

import os
import sys
import time
from scripts.athena_runner import (
    execute_query,
    fetch_query_results,
    get_athena_client,
    print_table,
)

DATABASE = "sales_db"
LOCAL_ROW_COUNT = 29666  # Verified from local PySpark data quality checks


def main():
    client = get_athena_client()
    bucket = os.getenv("S3_BUCKET_NAME", "sales-pipeline-rushi-2026-unique")

    print("\n=================================================================")
    print("STEP 1: MSCK REPAIR, SHOW PARTITIONS & ROW COUNT VERIFICATION")
    print("=================================================================")

    # 1. MSCK REPAIR TABLE
    repair_sql = f"MSCK REPAIR TABLE {DATABASE}.sales_curated;"
    print("\n[Executing] MSCK REPAIR TABLE...")
    q_id, metrics = execute_query(client, repair_sql, database=DATABASE)

    # 2. SHOW PARTITIONS
    print("\n[Executing] SHOW PARTITIONS...")
    q_id, metrics = execute_query(client, f"SHOW PARTITIONS {DATABASE}.sales_curated;", database=DATABASE)
    part_results = fetch_query_results(client, q_id, max_rows=100)
    print(f"Total partitions found: {len(part_results) - 1}")
    if part_results:
        print_table(part_results[0], part_results[1:10])  # Show sample partitions

    # 3. SELECT COUNT(*)
    print("\n[Executing] SELECT COUNT(*) on sales_curated...")
    count_sql = f"SELECT COUNT(*) AS total_records FROM {DATABASE}.sales_curated;"
    q_id, metrics = execute_query(client, count_sql, database=DATABASE)
    count_results = fetch_query_results(client, q_id)
    print_table(count_results[0], count_results[1:])

    athena_count = int(count_results[1][0])
    print(f"--> Athena Row Count: {athena_count:,}")
    print(f"--> Local PySpark Count: {LOCAL_ROW_COUNT:,}")
    if athena_count == LOCAL_ROW_COUNT:
        print("--> MATCH CONFIRMED: Athena partition count exactly matches local PySpark cleaned data!")
    else:
        print(f"--> NOTICE: Athena count ({athena_count}) differs from local ({LOCAL_ROW_COUNT}) by {athena_count - LOCAL_ROW_COUNT} rows.")

    print("\n=================================================================")
    print("STEP 2 & 3: RUNNING 5 ANALYTICAL QUERIES & CAPTURING EXECUTION METRICS")
    print("=================================================================")

    queries = [
        ("01_monthly_sales_trends", "sql/athena/01_monthly_sales_trends.sql"),
        ("02_top_profitable_products", "sql/athena/02_top_profitable_products.sql"),
        ("03_customer_rfm_segmentation", "sql/athena/03_customer_rfm_segmentation.sql"),
        ("04_shipping_performance_by_region", "sql/athena/04_shipping_performance_by_region.sql"),
        ("05_discount_impact_on_margins", "sql/athena/05_discount_impact_on_margins.sql"),
    ]

    analytical_stats = []
    for q_name, q_path in queries:
        print(f"\n--- Running: {q_name} ({q_path}) ---")
        with open(q_path, "r", encoding="utf-8") as f:
            sql_text = f.read()

        q_id, q_metrics = execute_query(client, sql_text, database=DATABASE)
        res = fetch_query_results(client, q_id, max_rows=10)
        if res:
            print_table(res[0], res[1:])

        analytical_stats.append({
            "name": q_name,
            "data_scanned_bytes": q_metrics["data_scanned_bytes"],
            "exec_time_ms": q_metrics["exec_time_ms"],
        })

    print("\n=================================================================")
    print("STEP 4: CREATE RAW CSV TABLE & RUN BENCHMARK COMPARISONS")
    print("=================================================================")

    raw_table_ddl = f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE}.sales_raw_csv (
        row_id STRING,
        order_id STRING,
        order_date STRING,
        ship_date STRING,
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
        sales STRING,
        quantity STRING,
        discount STRING,
        profit STRING
    )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
        'separatorChar' = ',',
        'quoteChar' = '\"',
        'escapeChar' = '\\\\'
    )
    LOCATION 's3://{bucket}/raw/'
    TBLPROPERTIES (
        'skip.header.line.count' = '1'
    );
    """
    print("\n[Executing] Creating raw CSV Athena external table...")
    execute_query(client, raw_table_ddl, database=DATABASE)

    # Benchmark 1: Full aggregation across the entire dataset (CSV vs Parquet)
    print("\n--- Benchmark 1A: Full Aggregation on RAW CSV ---")
    raw_agg_sql = f"""
    SELECT
        category,
        COUNT(*) AS total_lines,
        ROUND(SUM(TRY_CAST(sales AS DOUBLE)), 2) AS total_sales,
        ROUND(SUM(TRY_CAST(profit AS DOUBLE)), 2) AS total_profit
    FROM {DATABASE}.sales_raw_csv
    GROUP BY category
    ORDER BY total_sales DESC;
    """
    q_id_csv, csv_metrics = execute_query(client, raw_agg_sql, database=DATABASE)
    csv_res = fetch_query_results(client, q_id_csv)
    print_table(csv_res[0], csv_res[1:])

    print("\n--- Benchmark 1B: Full Aggregation on CURATED PARQUET ---")
    parquet_agg_sql = f"""
    SELECT
        category,
        COUNT(*) AS total_lines,
        ROUND(SUM(sales), 2) AS total_sales,
        ROUND(SUM(profit), 2) AS total_profit
    FROM {DATABASE}.sales_curated
    GROUP BY category
    ORDER BY total_sales DESC;
    """
    q_id_parquet, parquet_metrics = execute_query(client, parquet_agg_sql, database=DATABASE)
    parquet_res = fetch_query_results(client, q_id_parquet)
    print_table(parquet_res[0], parquet_res[1:])

    # Benchmark 2: Partition Pruning Test (Filter on 1 Year)
    print("\n--- Benchmark 2A: Filter by Year on RAW CSV (Full Scan) ---")
    raw_filter_sql = f"""
    SELECT
        category,
        COUNT(*) AS total_lines,
        ROUND(SUM(TRY_CAST(sales AS DOUBLE)), 2) AS total_sales,
        ROUND(SUM(TRY_CAST(profit AS DOUBLE)), 2) AS total_profit
    FROM {DATABASE}.sales_raw_csv
    WHERE order_date LIKE '2024%' OR order_date LIKE '%/2024' OR order_date LIKE '%-2024'
    GROUP BY category
    ORDER BY total_sales DESC;
    """
    q_id_csv_filtered, csv_filter_metrics = execute_query(client, raw_filter_sql, database=DATABASE)
    csv_filter_res = fetch_query_results(client, q_id_csv_filtered)
    print_table(csv_filter_res[0], csv_filter_res[1:])

    print("\n--- Benchmark 2B: Filter by Year on PARQUET (Partition Pruning Enabled) ---")
    parquet_filter_sql = f"""
    SELECT
        category,
        COUNT(*) AS total_lines,
        ROUND(SUM(sales), 2) AS total_sales,
        ROUND(SUM(profit), 2) AS total_profit
    FROM {DATABASE}.sales_curated
    WHERE order_year = 2024
    GROUP BY category
    ORDER BY total_sales DESC;
    """
    q_id_parquet_filtered, parquet_filter_metrics = execute_query(client, parquet_filter_sql, database=DATABASE)
    parquet_filter_res = fetch_query_results(client, q_id_parquet_filtered)
    print_table(parquet_filter_res[0], parquet_filter_res[1:])

    print("\n=================================================================")
    print("SUMMARY OF EMPIRICAL ATHENA MEASUREMENTS")
    print("=================================================================")
    print("\nAnalytical Queries on Curated Parquet:")
    for stat in analytical_stats:
        b = stat["data_scanned_bytes"]
        ms = stat["exec_time_ms"]
        print(f"- {stat['name']}: {b:,} bytes ({b/1024:.2f} KB) | {ms/1000.0:.2f}s")

    print("\nFormat & Partition Pruning Comparison:")
    print(f"- Full Aggregation CSV:     {csv_metrics['data_scanned_bytes']:,} bytes ({csv_metrics['data_scanned_bytes']/1024/1024:.2f} MB) | {csv_metrics['exec_time_ms']/1000.0:.2f}s")
    print(f"- Full Aggregation Parquet: {parquet_metrics['data_scanned_bytes']:,} bytes ({parquet_metrics['data_scanned_bytes']/1024:.2f} KB) | {parquet_metrics['exec_time_ms']/1000.0:.2f}s")
    reduction_pct = (1.0 - (parquet_metrics['data_scanned_bytes'] / csv_metrics['data_scanned_bytes'])) * 100
    print(f"  --> Data Scan Reduction: {reduction_pct:.2f}% reduction")

    print(f"- Filtered (1 Year) CSV:     {csv_filter_metrics['data_scanned_bytes']:,} bytes ({csv_filter_metrics['data_scanned_bytes']/1024/1024:.2f} MB) | {csv_filter_metrics['exec_time_ms']/1000.0:.2f}s")
    print(f"- Filtered (1 Year) Parquet: {parquet_filter_metrics['data_scanned_bytes']:,} bytes ({parquet_filter_metrics['data_scanned_bytes']/1024:.2f} KB) | {parquet_filter_metrics['exec_time_ms']/1000.0:.2f}s")
    filter_reduction_pct = (1.0 - (parquet_filter_metrics['data_scanned_bytes'] / csv_filter_metrics['data_scanned_bytes'])) * 100
    print(f"  --> Data Scan Reduction with Partition Pruning: {filter_reduction_pct:.2f}% reduction")


if __name__ == "__main__":
    main()
