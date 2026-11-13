-- Query 2: Top 3 Most Profitable Products per Category
-- Concepts: Window Functions (DENSE_RANK), Partitioning, Aggregation

WITH product_profitability AS (
    SELECT
        category,
        sub_category,
        product_id,
        product_name,
        SUM(quantity) AS total_units_sold,
        ROUND(SUM(sales), 2) AS total_sales,
        ROUND(SUM(profit), 2) AS total_profit,
        ROUND(SUM(profit) / NULLIF(SUM(sales), 0) * 100, 2) AS profit_margin_pct
    FROM sales_curated
    GROUP BY
        category,
        sub_category,
        product_id,
        product_name
),
ranked_products AS (
    SELECT
        category,
        sub_category,
        product_id,
        product_name,
        total_units_sold,
        total_sales,
        total_profit,
        profit_margin_pct,
        DENSE_RANK() OVER (
            PARTITION BY category 
            ORDER BY total_profit DESC
        ) AS rank_in_category
    FROM product_profitability
)
SELECT *
FROM ranked_products
WHERE rank_in_category <= 3
ORDER BY category, rank_in_category;
