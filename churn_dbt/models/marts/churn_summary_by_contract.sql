{{ config(materialized='table') }}

select
    contract_type,
    count(*) as customer_count,
    sum(case when churn_flag is true then 1 else 0 end) as churned_count,
    avg(case when churn_flag is true then 1.0 else 0.0 end) as churn_rate
from {{ ref('stg_customers_churn_clean') }}
group by contract_type
