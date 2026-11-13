-- Query 3: Customer Recency, Frequency, and Monetary (RFM) Segmentation
-- Concepts: Multi-CTE, NTILE Window Functions, Conditional Case Logic

WITH customer_aggregates AS (
    SELECT
        customer_id,
        MAX(customer_name) AS customer_name,
        MAX(segment) AS segment,
        MAX(order_date) AS last_order_date,
        DATE_DIFF('day', MAX(order_date), DATE '2025-12-31') AS recency_days,
        COUNT(DISTINCT order_id) AS frequency_orders,
        ROUND(SUM(sales), 2) AS monetary_total_spend,
        ROUND(SUM(profit), 2) AS total_customer_profit
    FROM sales_curated
    GROUP BY customer_id
),
rfm_scores AS (
    SELECT
        customer_id,
        customer_name,
        segment,
        recency_days,
        frequency_orders,
        monetary_total_spend,
        total_customer_profit,
        -- Lower recency days = better score (4 = most recent)
        NTILE(4) OVER (ORDER BY recency_days DESC) AS r_score,
        NTILE(4) OVER (ORDER BY frequency_orders ASC) AS f_score,
        NTILE(4) OVER (ORDER BY monetary_total_spend ASC) AS m_score
    FROM customer_aggregates
)
SELECT
    customer_id,
    customer_name,
    segment,
    recency_days,
    frequency_orders,
    monetary_total_spend,
    total_customer_profit,
    r_score,
    f_score,
    m_score,
    CASE
        WHEN r_score >= 3 AND f_score >= 3 AND m_score >= 3 THEN 'Champions'
        WHEN f_score >= 3 AND m_score >= 3 THEN 'Loyal Customers'
        WHEN r_score >= 3 AND f_score <= 2 THEN 'Promising / New Customers'
        WHEN r_score <= 2 AND f_score >= 3 THEN 'At Risk / Needs Attention'
        ELSE 'Hibernating'
    END AS customer_tier
FROM rfm_scores
ORDER BY monetary_total_spend DESC;
