{{ config(
    materialized = 'table'
) }}

WITH base AS (
    SELECT
        monthly_charges,
        churn_flag
    FROM {{ ref('stg_customers_churn_clean') }}
),

charge_buckets AS (
    SELECT
        CASE
            WHEN monthly_charges IS NULL THEN 'unknown'
            WHEN monthly_charges < 30 THEN '< 30'
            WHEN monthly_charges BETWEEN 30 AND 60 THEN '30–60'
            WHEN monthly_charges BETWEEN 61 AND 90 THEN '61–90'
            ELSE '> 90'
        END AS charge_bucket,
        churn_flag
    FROM base
)

SELECT
    charge_bucket,
    COUNT(*) AS total_customers,
    SUM(CASE WHEN churn_flag THEN 1 ELSE 0 END) AS churned_customers,
    ROUND(
        CASE WHEN COUNT(*) > 0
             THEN SUM(CASE WHEN churn_flag THEN 1 ELSE 0 END)::numeric / COUNT(*)
             ELSE 0
        END,
        4
    ) AS churn_rate
FROM charge_buckets
GROUP BY charge_bucket
ORDER BY
    CASE charge_bucket
        WHEN 'unknown' THEN 0
        WHEN '< 30' THEN 1
        WHEN '30–60' THEN 2
        WHEN '61–90' THEN 3
        WHEN '> 90' THEN 4
        ELSE 99
    END
