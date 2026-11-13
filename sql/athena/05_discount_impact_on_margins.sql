-- Query 5: Discount Tier Impact on Sales Volume and Profitability
-- Concepts: CASE bucketing, Grouping Sets / Aggregations, Elasticity Analysis

WITH discount_buckets AS (
    SELECT
        order_id,
        category,
        sales,
        profit,
        quantity,
        discount,
        CASE
            WHEN discount = 0.0 THEN '0% (No Discount)'
            WHEN discount > 0.0 AND discount <= 0.10 THEN '1% - 10%'
            WHEN discount > 0.10 AND discount <= 0.20 THEN '11% - 20%'
            WHEN discount > 0.20 AND discount <= 0.35 THEN '21% - 35%'
            ELSE '> 35% Deep Discount'
        END AS discount_tier
    FROM sales_curated
)
SELECT
    category,
    discount_tier,
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(quantity) AS total_quantity,
    ROUND(SUM(sales), 2) AS total_revenue,
    ROUND(SUM(profit), 2) AS total_profit,
    ROUND(SUM(profit) / NULLIF(SUM(sales), 0) * 100, 2) AS net_margin_pct
FROM discount_buckets
GROUP BY
    category,
    discount_tier
ORDER BY
    category,
    discount_tier;
