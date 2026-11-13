-- Staging view: Standardizes raw sales data from Snowflake RAW layer
-- Granularity: One record per order line item

WITH source_data AS (
    SELECT * FROM {{ source('raw_sales', 'raw_sales_parquet') }}
),

renamed_and_cast AS (
    SELECT
        -- Surrogate Key (Hash of natural business keys)
        MD5(CONCAT(COALESCE(order_id, ''), '-', COALESCE(product_id, ''))) AS sales_item_key,

        -- Natural / Business IDs
        order_id,
        product_id,
        customer_id,

        -- Transaction Dates
        CAST(order_date AS DATE) AS order_date,
        CAST(ship_date AS DATE) AS ship_date,
        DATEDIFF('day', CAST(order_date AS DATE), CAST(ship_date AS DATE)) AS days_to_ship,

        -- Attributes
        TRIM(ship_mode) AS ship_mode,
        TRIM(customer_name) AS customer_name,
        TRIM(segment) AS segment,
        TRIM(country) AS country,
        TRIM(city) AS city,
        TRIM(state) AS state,
        TRIM(postal_code) AS postal_code,
        TRIM(region) AS region,
        TRIM(category) AS category,
        TRIM(sub_category) AS sub_category,
        TRIM(product_name) AS product_name,

        -- Financial Metrics
        CAST(sales AS NUMBER(12, 2)) AS sales_amount,
        CAST(quantity AS INTEGER) AS quantity,
        CAST(discount AS NUMBER(5, 4)) AS discount_rate,
        CAST(profit AS NUMBER(12, 2)) AS profit_amount,
        CAST(cost AS NUMBER(12, 2)) AS cost_amount,
        CAST(profit_margin AS NUMBER(6, 4)) AS profit_margin,

        -- Metadata
        order_year,
        order_month,
        ingestion_timestamp,
        load_timestamp
    FROM source_data
    WHERE order_id IS NOT NULL 
      AND product_id IS NOT NULL
)

SELECT * FROM renamed_and_cast
