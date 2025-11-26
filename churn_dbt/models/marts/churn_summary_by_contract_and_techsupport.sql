{{ config(
    materialized = 'table'
) }}

WITH base AS (
    SELECT
        contract_type,
        tech_support,
        churn_flag
    FROM {{ ref('stg_customers_churn_clean') }}
)

SELECT
    COALESCE(contract_type, 'unknown') AS contract_type,
    COALESCE(tech_support, 'unknown') AS tech_support,
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
GROUP BY
    COALESCE(contract_type, 'unknown'),
    COALESCE(tech_support, 'unknown')
ORDER BY
    contract_type,
    tech_support
