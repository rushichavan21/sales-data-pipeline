-- ==============================================================================
-- Snowflake Environment Setup for Sales Data Pipeline
-- Compliant with Project Cost and Safety Rules:
-- 1. Uses X-Small warehouse (0.0003 credits/sec = 1 credit/hr)
-- 2. AUTO_SUSPEND = 60 seconds (Warehouse turns off immediately after 1 min idle)
-- 3. INITIALLY_SUSPENDED = TRUE (Zero idle billing on creation)
-- ==============================================================================

USE ROLE ACCOUNTADMIN;

-- 1. Create Roles and Users
CREATE ROLE IF NOT EXISTS SALES_DEV_ROLE;
GRANT ROLE SALES_DEV_ROLE TO ROLE ACCOUNTADMIN;

-- 2. Create Minimal Virtual Warehouse (X-Small, 60s auto-suspend)
CREATE WAREHOUSE IF NOT EXISTS SALES_WH
    WITH 
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Minimal X-Small warehouse with 60s auto-suspend for Sales Pipeline';

GRANT USAGE ON WAREHOUSE SALES_WH TO ROLE SALES_DEV_ROLE;

-- 3. Create Dedicated Database and Schemas
CREATE DATABASE IF NOT EXISTS SALES_DW;
GRANT ALL ON DATABASE SALES_DW TO ROLE SALES_DEV_ROLE;

USE DATABASE SALES_DW;

CREATE SCHEMA IF NOT EXISTS RAW;
CREATE SCHEMA IF NOT EXISTS STAGING;
CREATE SCHEMA IF NOT EXISTS MARTS;

GRANT ALL ON SCHEMA RAW TO ROLE SALES_DEV_ROLE;
GRANT ALL ON SCHEMA STAGING TO ROLE SALES_DEV_ROLE;
GRANT ALL ON SCHEMA MARTS TO ROLE SALES_DEV_ROLE;

-- 4. Parquet File Format Definition
USE SCHEMA RAW;

CREATE OR REPLACE FILE FORMAT PARQUET_FORMAT
    TYPE = 'PARQUET'
    COMPRESSION = 'SNAPPY';

-- 5. Staging External Stage (pointing to Curated S3 Bucket)
-- NOTE: Replace {{bucket_name}} with your actual S3 bucket name
CREATE OR REPLACE STAGE S3_CURATED_STAGE
    URL = 's3://{{bucket_name}}/curated/sales_curated/'
    FILE_FORMAT = PARQUET_FORMAT;

-- 6. Raw Ingestion Table (stores Parquet records for dbt source transformation)
CREATE TABLE IF NOT EXISTS RAW_SALES_PARQUET (
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
    ingestion_timestamp TIMESTAMP_NTZ,
    order_year INT,
    order_month INT,
    file_name STRING,
    load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- 7. Copy Command to Ingest Curated Parquet into RAW table
-- (Executed after S3 curated data is uploaded)
/*
COPY INTO RAW_SALES_PARQUET (
    row_id, order_id, order_date, ship_date, ship_mode,
    customer_id, customer_name, segment, country, city,
    state, postal_code, region, product_id, category,
    sub_category, product_name, sales, quantity, discount,
    profit, cost, profit_margin, ingestion_timestamp,
    order_year, order_month, file_name
)
FROM (
    SELECT 
        $1:row_id::STRING,
        $1:order_id::STRING,
        $1:order_date::DATE,
        $1:ship_date::DATE,
        $1:ship_mode::STRING,
        $1:customer_id::STRING,
        $1:customer_name::STRING,
        $1:segment::STRING,
        $1:country::STRING,
        $1:city::STRING,
        $1:state::STRING,
        $1:postal_code::STRING,
        $1:region::STRING,
        $1:product_id::STRING,
        $1:category::STRING,
        $1:sub_category::STRING,
        $1:product_name::STRING,
        $1:sales::DOUBLE,
        $1:quantity::INT,
        $1:discount::DOUBLE,
        $1:profit::DOUBLE,
        $1:cost::DOUBLE,
        $1:profit_margin::DOUBLE,
        $1:ingestion_timestamp::TIMESTAMP_NTZ,
        METADATA$FILENAME,
        SPLIT_PART(SPLIT_PART(METADATA$FILENAME, 'order_year=', 2), '/', 1)::INT,
        SPLIT_PART(SPLIT_PART(METADATA$FILENAME, 'order_month=', 2), '/', 1)::INT
    FROM @S3_CURATED_STAGE
)
FILE_FORMAT = (FORMAT_NAME = 'PARQUET_FORMAT')
ON_ERROR = 'CONTINUE';
*/

-- ==============================================================================
-- Teardown Script (run when tearing down project resources to avoid cost)
-- ==============================================================================
/*
USE ROLE ACCOUNTADMIN;
DROP DATABASE IF EXISTS SALES_DW;
DROP WAREHOUSE IF EXISTS SALES_WH;
DROP ROLE IF EXISTS SALES_DEV_ROLE;
*/
