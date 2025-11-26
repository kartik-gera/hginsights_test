{{ config(
    materialized = 'table'
) }}

WITH base AS (
    SELECT
        age,
        churn_flag
    FROM {{ ref('stg_customers_churn_clean') }}
),

age_buckets AS (
    SELECT
        CASE
            WHEN age IS NULL THEN 'unknown'
            WHEN age < 25 THEN '< 25'
            WHEN age BETWEEN 25 AND 34 THEN '25–34'
            WHEN age BETWEEN 35 AND 49 THEN '35–49'
            WHEN age BETWEEN 50 AND 64 THEN '50–64'
            ELSE '65+'
        END AS age_segment,
        churn_flag
    FROM base
)

SELECT
    age_segment,
    COUNT(*) AS total_customers,
    SUM(CASE WHEN churn_flag THEN 1 ELSE 0 END) AS churned_customers,
    ROUND(
        CASE WHEN COUNT(*) > 0
             THEN SUM(CASE WHEN churn_flag THEN 1 ELSE 0 END)::numeric / COUNT(*)
             ELSE 0
        END,
        4
    ) AS churn_rate
FROM age_buckets
GROUP BY age_segment
ORDER BY
    CASE age_segment
        WHEN 'unknown' THEN 0
        WHEN '< 25' THEN 1
        WHEN '25–34' THEN 2
        WHEN '35–49' THEN 3
        WHEN '50–64' THEN 4
        WHEN '65+' THEN 5
        ELSE 99
    END