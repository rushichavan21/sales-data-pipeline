-- Dimension: Product Catalog
-- Granularity: 1 row per unique product SKU

WITH staging AS (
    SELECT * FROM {{ ref('stg_sales') }}
),

distinct_products AS (
    SELECT
        product_id,
        category,
        sub_category,
        product_name,
        ROW_NUMBER() OVER (
            PARTITION BY product_id 
            ORDER BY order_date DESC, ingestion_timestamp DESC
        ) AS row_num
    FROM staging
),

final AS (
    SELECT
        MD5(product_id) AS product_key,
        product_id,
        category,
        sub_category,
        product_name,
        CURRENT_TIMESTAMP() AS dbt_updated_at
    FROM distinct_products
    WHERE row_num = 1
)

SELECT * FROM final
