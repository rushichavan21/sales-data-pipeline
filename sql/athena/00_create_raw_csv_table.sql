-- Athena DDL: External Table over Raw CSV (for performance benchmarking)
-- Replace {{bucket_name}} with your actual S3 bucket name

CREATE EXTERNAL TABLE IF NOT EXISTS sales_db.sales_raw_csv (
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
    'escapeChar' = '\\'
)
LOCATION 's3://{{bucket_name}}/raw/'
TBLPROPERTIES (
    'skip.header.line.count' = '1'
);
