-- Dimension: Calendar Date
-- Granularity: 1 row per calendar day across the sales history window

WITH date_spine AS (
    SELECT 
        DATEADD(day, SEQ4(), '2020-01-01'::DATE) AS calendar_date
    FROM TABLE(GENERATOR(ROWCOUNT => 365 * 11)) -- 11 years (2020 - 2030)
),

enriched_dates AS (
    SELECT
        -- Date Key (Format: YYYYMMDD)
        TO_NUMBER(TO_CHAR(calendar_date, 'YYYYMMDD')) AS date_key,
        calendar_date,
        YEAR(calendar_date) AS calendar_year,
        QUARTER(calendar_date) AS calendar_quarter,
        CONCAT('Q', QUARTER(calendar_date), ' ', YEAR(calendar_date)) AS year_quarter,
        MONTH(calendar_date) AS calendar_month,
        MONTHNAME(calendar_date) AS month_name,
        WEEK(calendar_date) AS week_of_year,
        DAYOFMONTH(calendar_date) AS day_of_month,
        DAYOFWEEK(calendar_date) AS day_of_week,
        DAYNAME(calendar_date) AS day_name,
        CASE WHEN DAYOFWEEK(calendar_date) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
        CASE WHEN DAYOFWEEK(calendar_date) NOT IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekday
    FROM date_spine
)

SELECT * FROM enriched_dates
