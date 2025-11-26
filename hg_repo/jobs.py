from dagster import AssetSelection, ScheduleDefinition, define_asset_job

from hg_repo.assets import (
    raw_customers,
    dbt_build,
    analytics_freshness_check,
)

from .config import CHURN_CRON

# Job: run the whole churn pipeline
churn_pipeline_job = define_asset_job(
    name="churn_pipeline_job",
    selection=AssetSelection.assets(
        "raw_customers",
        "dbt_build",
        "analytics_freshness_check",
    ),
)

# Schedule: default = every hour, but configurable via env var
churn_hourly_schedule = ScheduleDefinition(
    name="churn_hourly_schedule",
    job=churn_pipeline_job,
    cron_schedule=CHURN_CRON,
)
