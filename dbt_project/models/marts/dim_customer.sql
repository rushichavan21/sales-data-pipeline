-- Dimension: Customer
-- Granularity: 1 row per unique customer

WITH staging AS (
    SELECT * FROM {{ ref('stg_sales') }}
),

distinct_customers AS (
    SELECT
        customer_id,
        customer_name,
        segment,
        country,
        city,
        state,
        postal_code,
        region,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id 
            ORDER BY order_date DESC, ingestion_timestamp DESC
        ) AS row_num
    FROM staging
),

final AS (
    SELECT
        MD5(customer_id) AS customer_key,
        customer_id,
        customer_name,
        segment,
        country,
        city,
        state,
        postal_code,
        region,
        CURRENT_TIMESTAMP() AS dbt_updated_at
    FROM distinct_customers
    WHERE row_num = 1
)

SELECT * FROM final
