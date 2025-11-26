{{ config(
    materialized = 'table'
) }}

WITH base AS (
    SELECT
        gender,
        churn_flag
    FROM {{ ref('stg_customers_churn_clean') }}
)

SELECT
    COALESCE(gender, 'unknown') AS gender,
    COUNT(*) AS total_customers,
    SUM(CASE WHEN churn_flag THEN 1 ELSE 0 END) AS churned_customers,
    ROUND(
        CASE WHEN COUNT(*) > 0
             THEN SUM(CASE WHEN churn_flag THEN 1 ELSE 0 END)::numeric / COUNT(*)
             ELSE 0
        END,
        4
    ) AS churn_rate
FROM base
GROUP BY COALESCE(gender, 'unknown')
ORDER BY total_customers DESC