-- Query 4: Shipping Performance, Lead Times, and SLA Compliance by Region
-- Concepts: Date Arithmetic, Conditional Aggregation, Ratio Calculations

WITH shipment_metrics AS (
    SELECT
        order_id,
        region,
        state,
        ship_mode,
        order_date,
        ship_date,
        DATE_DIFF('day', order_date, ship_date) AS lead_time_days,
        CASE
            WHEN ship_mode = 'Same Day' AND DATE_DIFF('day', order_date, ship_date) > 0 THEN 1
            WHEN ship_mode = 'First Class' AND DATE_DIFF('day', order_date, ship_date) > 2 THEN 1
            WHEN ship_mode = 'Second Class' AND DATE_DIFF('day', order_date, ship_date) > 4 THEN 1
            WHEN ship_mode = 'Standard Class' AND DATE_DIFF('day', order_date, ship_date) > 6 THEN 1
            ELSE 0
        END AS is_delayed
    FROM sales_curated
)
SELECT
    region,
    ship_mode,
    COUNT(DISTINCT order_id) AS total_shipments,
    ROUND(AVG(lead_time_days), 1) AS avg_lead_time_days,
    MIN(lead_time_days) AS min_lead_time_days,
    MAX(lead_time_days) AS max_lead_time_days,
    SUM(is_delayed) AS delayed_shipments,
    ROUND(CAST(SUM(is_delayed) AS DOUBLE) / COUNT(DISTINCT order_id) * 100, 2) AS delay_rate_pct
FROM shipment_metrics
GROUP BY
    region,
    ship_mode
ORDER BY
    region,
    delay_rate_pct DESC;
