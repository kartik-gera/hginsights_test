{{ config(materialized='table') }}

with src as (
    select
        customer_id,
        age,
        gender,
        tenure,
        monthly_charges,
        contract_type,
        internet_service,
        tech_support,
        total_charges,
        churn
    from raw.customers_churn
)

select
    md5(src.customer_id::text) as customer_id,   -- anonymised PII
    coalesce(src.age, 0) as age,
    case
        when src.gender is null or trim(src.gender) = '' then 'unknown'
        else lower(trim(src.gender))
    end as gender,
    coalesce(src.tenure, 0) as tenure,
    coalesce(src.monthly_charges, 0.0) as monthly_charges,
    src.contract_type,
    src.internet_service,
    src.tech_support,
    coalesce(src.total_charges, 0.0) as total_charges,
    case
        when lower(trim(src.churn)) = 'yes' then true
        when lower(trim(src.churn)) = 'no'  then false
        else null
    end as churn_flag
from src
