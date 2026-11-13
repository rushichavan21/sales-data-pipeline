-- Fact Table: Retail Sales Transactions
-- Granularity: 1 row per order item line
-- Schema: Kimball Star Schema Fact Table

WITH staging AS (
    SELECT * FROM {{ ref('stg_sales') }}
),

dim_cust AS (
    SELECT customer_key, customer_id FROM {{ ref('dim_customer') }}
),

dim_prod AS (
    SELECT product_key, product_id FROM {{ ref('dim_product') }}
),

dim_dt AS (
    SELECT date_key, calendar_date FROM {{ ref('dim_date') }}
),

fact AS (
    SELECT
        -- Primary Surrogate Key
        s.sales_item_key,

        -- Foreign Dimension Keys
        c.customer_key,
        p.product_key,
        d_order.date_key AS order_date_key,
        d_ship.date_key AS ship_date_key,

        -- Degenerate Dimensions
        s.order_id,
        s.ship_mode,

        -- Facts & Additive Metrics
        s.sales_amount,
        s.quantity,
        s.discount_rate,
        ROUND(s.sales_amount * s.discount_rate, 2) AS discount_amount,
        s.cost_amount,
        s.profit_amount,
        s.profit_margin,
        s.days_to_ship,

        -- Lineage & Metadata
        s.order_year,
        s.order_month,
        s.ingestion_timestamp,
        CURRENT_TIMESTAMP() AS dbt_loaded_at
    FROM staging s
    INNER JOIN dim_cust c 
        ON s.customer_id = c.customer_id
    INNER JOIN dim_prod p 
        ON s.product_id = p.product_id
    LEFT JOIN dim_dt d_order 
        ON s.order_date = d_order.calendar_date
    LEFT JOIN dim_dt d_ship 
        ON s.ship_date = d_ship.calendar_date
)

SELECT * FROM fact
