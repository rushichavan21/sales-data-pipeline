-- Query 1: Monthly Sales, Profit Trends, and MoM Growth
-- Concepts: CTEs, Window Functions (LAG), Aggregations

WITH monthly_summary AS (
    SELECT
        order_year,
        order_month,
        DATE(CONCAT(CAST(order_year AS VARCHAR), '-', LPAD(CAST(order_month AS VARCHAR), 2, '0'), '-01')) AS month_start,
        COUNT(DISTINCT order_id) AS total_orders,
        SUM(quantity) AS total_units_sold,
        ROUND(SUM(sales), 2) AS total_revenue,
        ROUND(SUM(profit), 2) AS total_profit,
        ROUND(AVG(profit_margin) * 100, 2) AS avg_profit_margin_pct
    FROM sales_curated
    GROUP BY
        order_year,
        order_month
),
mom_trends AS (
    SELECT
        order_year,
        order_month,
        month_start,
        total_orders,
        total_units_sold,
        total_revenue,
        total_profit,
        avg_profit_margin_pct,
        LAG(total_revenue, 1) OVER (ORDER BY month_start) AS prev_month_revenue,
        ROUND(
            (total_revenue - LAG(total_revenue, 1) OVER (ORDER BY month_start)) 
            / NULLIF(LAG(total_revenue, 1) OVER (ORDER BY month_start), 0) * 100, 
            2
        ) AS revenue_growth_pct
    FROM monthly_summary
)
SELECT *
FROM mom_trends
ORDER BY month_start DESC;
