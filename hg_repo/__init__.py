# hg_repo/__init__.py
from dagster import Definitions

from hg_repo.assets import (
    raw_customers,
    dbt_build,
    analytics_freshness_check,
)

from hg_repo.jobs import churn_pipeline_job, churn_hourly_schedule
from hg_repo.sensors import churn_file_sensor

defs = Definitions(
    assets=[raw_customers, dbt_build, analytics_freshness_check],
    jobs=[churn_pipeline_job],
    schedules=[churn_hourly_schedule],
    sensors=[churn_file_sensor],
)
