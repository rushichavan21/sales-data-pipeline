"""Sales Data Cleaning & Transformation Pipeline.

Developed to run both locally (PySpark on developer machines) and in the cloud (AWS Glue Spark job).
Cleans raw retail sales data, enforces schema, handles nulls and anomalies,
derives analytics columns, performs data quality checks, and writes partitioned Parquet.
"""

import logging
import os
import sys
from typing import Dict, List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("SalesDataCleaner")


def init_spark_session(
    app_name: str = "SalesDataPipeline-Cleaner",
) -> Tuple[SparkSession, Optional[object], Optional[object]]:
    """Initialize SparkSession portably across Windows local dev and AWS Glue.

    Returns:
        tuple: (SparkSession, glue_context, glue_job)
    """
    glue_context = None
    glue_job = None

    # Check if running in AWS Glue environment
    try:
        from awsglue.context import GlueContext  # type: ignore
        from awsglue.job import Job  # type: ignore

        spark = SparkSession.builder.appName(app_name).getOrCreate()
        glue_context = GlueContext(spark.sparkContext)
        glue_job = Job(glue_context)
        logger.info("Initialized AWS Glue execution context.")
        return spark, glue_context, glue_job
    except ImportError:
        logger.info("AWS Glue libraries not detected. Configuring local Spark session.")

    # Local development configuration (specifically handling Windows environment)
    if sys.platform == "win32":
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        hadoop_dir = os.path.join(project_root, "hadoop")
        if os.path.exists(hadoop_dir):
            os.environ["HADOOP_HOME"] = hadoop_dir
            bin_path = os.path.join(hadoop_dir, "bin")
            if bin_path not in os.environ.get("PATH", ""):
                os.environ["PATH"] = f"{bin_path};{os.environ.get('PATH', '')}"
            logger.info("Configured HADOOP_HOME for local Windows execution: %s", hadoop_dir)

        # Ensure Python worker uses active interpreter
        os.environ["PYSPARK_PYTHON"] = sys.executable
        os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    spark = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info("Local SparkSession created successfully.")
    return spark, glue_context, glue_job


def get_raw_schema() -> StructType:
    """Return explicit schema for raw Superstore sales CSV."""
    return StructType(
        [
            StructField("row_id", StringType(), True),
            StructField("order_id", StringType(), True),
            StructField("order_date", StringType(), True),
            StructField("ship_date", StringType(), True),
            StructField("ship_mode", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("customer_name", StringType(), True),
            StructField("segment", StringType(), True),
            StructField("country", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("postal_code", StringType(), True),
            StructField("region", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("category", StringType(), True),
            StructField("sub_category", StringType(), True),
            StructField("product_name", StringType(), True),
            StructField("sales", StringType(), True),
            StructField("quantity", StringType(), True),
            StructField("discount", StringType(), True),
            StructField("profit", StringType(), True),
        ]
    )


def read_raw_data(spark: SparkSession, input_path: str) -> DataFrame:
    """Read raw sales CSV data with explicit schema."""
    logger.info("Reading raw data from: %s", input_path)
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .schema(get_raw_schema())
        .load(input_path)
    )
    return df


def clean_and_standardize_data(df: DataFrame) -> DataFrame:
    """Clean, parse, standardize types, handle nulls, and deduplicate records.

    Transformations:
    - Trim whitespace across all string columns
    - Standardize dates supporting multiple formats (yyyy-MM-dd, MM/dd/yyyy, dd-MM-yyyy)
    - Clean currency symbols and parse numeric values
    - Impute missing customer names and postal codes
    - Filter unrecoverable records (null order_id, null product_id, non-positive sales/quantity)
    - Deduplicate identical order_id + product_id lines
    """
    logger.info("Beginning data cleaning and standardization...")

    # 1. Trim leading and trailing whitespace on text columns
    string_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
    cleaned_df = df
    for c in string_cols:
        cleaned_df = cleaned_df.withColumn(c, F.trim(F.col(c)))

    # 2. Standardize dates to DateType using multi-pattern coalesce
    order_date_parsed = F.coalesce(
        F.to_date(F.col("order_date"), "yyyy-MM-dd"),
        F.to_date(F.col("order_date"), "MM/dd/yyyy"),
        F.to_date(F.col("order_date"), "dd-MM-yyyy"),
    )
    ship_date_parsed = F.coalesce(
        F.to_date(F.col("ship_date"), "yyyy-MM-dd"),
        F.to_date(F.col("ship_date"), "MM/dd/yyyy"),
        F.to_date(F.col("ship_date"), "dd-MM-yyyy"),
    )

    cleaned_df = (
        cleaned_df.withColumn("order_date", order_date_parsed)
        .withColumn("ship_date", ship_date_parsed)
    )

    # 3. Clean numeric fields (strip $ signs, commas, parse float/int)
    cleaned_sales = F.regexp_replace(F.col("sales"), r"[\$,]", "").cast(DoubleType())
    cleaned_qty = F.col("quantity").cast(IntegerType())
    cleaned_discount = F.col("discount").cast(DoubleType())
    cleaned_profit = F.col("profit").cast(DoubleType())

    cleaned_df = (
        cleaned_df.withColumn("sales", cleaned_sales)
        .withColumn("quantity", cleaned_qty)
        .withColumn("discount", cleaned_discount)
        .withColumn("profit", cleaned_profit)
    )

    # 4. Handle nulls and default values
    cleaned_df = (
        cleaned_df.withColumn(
            "customer_name",
            F.when(
                F.col("customer_name").isNull() | (F.col("customer_name") == ""),
                F.lit("Unknown Customer"),
            ).otherwise(F.col("customer_name")),
        )
        .withColumn(
            "postal_code",
            F.when(
                F.col("postal_code").isNull() | (F.col("postal_code") == ""),
                F.lit("00000"),
            ).otherwise(F.col("postal_code")),
        )
        .withColumn(
            "segment",
            F.when(
                F.col("segment").isNull() | (F.col("segment") == ""),
                F.lit("Consumer"),
            ).otherwise(F.col("segment")),
        )
    )

    # 5. Filter unrecoverable records: must have order_id, product_id, valid order_date
    cleaned_df = cleaned_df.filter(
        F.col("order_id").isNotNull()
        & (F.col("order_id") != "")
        & F.col("product_id").isNotNull()
        & (F.col("product_id") != "")
        & F.col("order_date").isNotNull()
        & (F.col("sales") > 0.0)
        & (F.col("quantity") > 0)
    )

    # 6. Deduplicate based on order_id and product_id (natural business key)
    cleaned_df = cleaned_df.dropDuplicates(["order_id", "product_id"])

    return cleaned_df


def enrich_sales_data(df: DataFrame) -> DataFrame:
    """Derive analytics columns: partition keys, cost, and profit margin.

    Derived columns:
    - order_year: Integer year for S3 partitioning
    - order_month: Integer month for S3 partitioning
    - cost: derived cost = sales * (1 - discount) - profit (rounded to 2 decimals)
    - profit_margin: profit / sales ratio (rounded to 4 decimals)
    - ingestion_timestamp: current UTC timestamp when processed
    """
    logger.info("Enriching sales data with derived metrics and partition keys...")
    enriched_df = (
        df.withColumn("order_year", F.year(F.col("order_date")))
        .withColumn("order_month", F.month(F.col("order_date")))
        .withColumn(
            "cost",
            F.round(
                (F.col("sales") * (F.lit(1.0) - F.coalesce(F.col("discount"), F.lit(0.0))))
                - F.col("profit"),
                2,
            ),
        )
        .withColumn(
            "profit_margin",
            F.when(
                F.col("sales") > 0.0,
                F.round(F.col("profit") / F.col("sales"), 4),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn("ingestion_timestamp", F.current_timestamp())
    )
    return enriched_df


def run_data_quality_checks(initial_count: int, cleaned_df: DataFrame) -> Dict[str, object]:
    """Execute validation and quality assertions on cleaned data."""
    logger.info("Executing data quality checks...")
    final_count = cleaned_df.count()

    # Null checks on mandatory columns
    critical_columns = ["order_id", "product_id", "order_date", "sales", "quantity", "order_year", "order_month"]
    null_counts = {}
    for col_name in critical_columns:
        null_cnt = cleaned_df.filter(F.col(col_name).isNull()).count()
        null_counts[col_name] = null_cnt

    # Value sanity check
    invalid_sales = cleaned_df.filter(F.col("sales") <= 0.0).count()
    invalid_qty = cleaned_df.filter(F.col("quantity") <= 0).count()

    metrics = {
        "initial_row_count": initial_count,
        "final_row_count": final_count,
        "dropped_rows": initial_count - final_count,
        "retention_rate_pct": round((final_count / initial_count * 100), 2) if initial_count > 0 else 0,
        "null_counts": null_counts,
        "invalid_sales_count": invalid_sales,
        "invalid_quantity_count": invalid_qty,
    }

    logger.info("Data Quality Metrics: %s", metrics)

    # Assertions
    if final_count == 0:
        raise ValueError("Data quality check failed: Cleaned DataFrame is empty!")
    for c, cnt in null_counts.items():
        if cnt > 0:
            raise ValueError(f"Data quality check failed: Found {cnt} null values in critical column '{c}'!")
    if invalid_sales > 0 or invalid_qty > 0:
        raise ValueError("Data quality check failed: Non-positive sales or quantity values detected!")

    return metrics


def write_curated_parquet(
    df: DataFrame,
    output_path: str,
    partition_cols: Optional[List[str]] = None,
) -> None:
    """Write DataFrame as partitioned Parquet with Snappy compression."""
    if partition_cols is None:
        partition_cols = ["order_year", "order_month"]

    logger.info(
        "Writing curated Parquet data to: %s (partitioned by: %s)",
        output_path,
        partition_cols,
    )

    (
        df.write.mode("overwrite")
        .partitionBy(*partition_cols)
        .parquet(output_path)
    )
    logger.info("Curated Parquet write completed successfully.")


def run_pipeline(input_path: str, output_path: str) -> Dict[str, object]:
    """Execute complete end-to-end cleaning pipeline."""
    spark, glue_context, glue_job = init_spark_session()
    try:
        raw_df = read_raw_data(spark, input_path)
        initial_count = raw_df.count()
        logger.info("Initial raw records ingested: %d", initial_count)

        cleaned_df = clean_and_standardize_data(raw_df)
        enriched_df = enrich_sales_data(cleaned_df)

        metrics = run_data_quality_checks(initial_count, enriched_df)
        write_curated_parquet(enriched_df, output_path)

        if glue_job is not None:
            glue_job.commit()

        return metrics
    finally:
        spark.stop()
        logger.info("Spark session terminated.")


def parse_arguments() -> Tuple[str, str]:
    """Parse CLI arguments supporting both local execution and Glue parameter passing."""
    import argparse

    parser = argparse.ArgumentParser(description="Sales Data Cleaning Pipeline")
    parser.add_argument(
        "--input_path",
        default="data/raw/superstore_sales_raw.csv",
        help="Path to raw CSV sales data (local path or s3://...)",
    )
    parser.add_argument(
        "--output_path",
        default="data/curated/sales_curated",
        help="Path for curated Parquet output (local path or s3://...)",
    )

    # Check if invoked via AWS Glue getResolvedOptions
    try:
        from awsglue.utils import getResolvedOptions  # type: ignore

        glue_args = getResolvedOptions(
            sys.argv, ["JOB_NAME", "input_path", "output_path"]
        )
        return glue_args["input_path"], glue_args["output_path"]
    except Exception:
        pass

    args, _ = parser.parse_known_args()
    return args.input_path, args.output_path


if __name__ == "__main__":
    input_p, output_p = parse_arguments()
    logger.info("Starting pipeline with input=%s, output=%s", input_p, output_p)
    run_pipeline(input_p, output_p)
