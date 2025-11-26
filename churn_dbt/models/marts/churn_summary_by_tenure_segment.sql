{{ config(
    materialized = 'table'
) }}

WITH base AS (
    SELECT
        tenure,
        churn_flag
    FROM {{ ref('stg_customers_churn_clean') }}
),

tenure_buckets AS (
    SELECT
        CASE
            WHEN tenure IS NULL THEN 'unknown'
            WHEN tenure < 12 THEN '< 12 months'
            WHEN tenure BETWEEN 12 AND 24 THEN '12–24 months'
            WHEN tenure BETWEEN 25 AND 48 THEN '25–48 months'
            ELSE '> 48 months'
        END AS tenure_segment,
        churn_flag
    FROM base
)

SELECT
    tenure_segment,
    COUNT(*) AS total_customers,
    SUM(CASE WHEN churn_flag THEN 1 ELSE 0 END) AS churned_customers,
    ROUND(
        CASE WHEN COUNT(*) > 0
             THEN SUM(CASE WHEN churn_flag THEN 1 ELSE 0 END)::numeric / COUNT(*)
             ELSE 0
        END,
        4
    ) AS churn_rate
FROM tenure_buckets
GROUP BY tenure_segment
ORDER BY
    CASE tenure_segment
        WHEN 'unknown' THEN 0
        WHEN '< 12 months' THEN 1
        WHEN '12–24 months' THEN 2
        WHEN '25–48 months' THEN 3
        WHEN '> 48 months' THEN 4
        ELSE 99
    END
