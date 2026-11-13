-- Athena DDL: External Table over Curated Partitioned Parquet
-- Replace {{bucket_name}} with your actual S3 bucket name

CREATE EXTERNAL TABLE IF NOT EXISTS sales_curated (
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
LOCATION 's3://{{bucket_name}}/curated/sales_curated/'
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY'
);

-- Recover partitions automatically from the S3 folder structure
MSCK REPAIR TABLE sales_curated;
