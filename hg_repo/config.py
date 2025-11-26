import os
from typing import Iterable
from dotenv import load_dotenv
import pandas as pd
import psycopg2
from dagster import Failure, RetryRequested

# Load environment variables from .env file
load_dotenv()

# Warehouse DB (ONLY raw, stg, analytics)
POSTGRES_DATA_WAREHOUSE = os.getenv("POSTGRES_DATA_WAREHOUSE", "hg_dw")
POSTGRES_USER = os.getenv("POSTGRES_USER", "hg_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "hg_password")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))

# Retry config for dbt and DB connections
DBT_MAX_RETRIES = int(os.getenv("DBT_MAX_RETRIES", "3"))
DBT_RETRY_WAIT_SECONDS = int(os.getenv("DBT_RETRY_WAIT_SECONDS", "30"))

# Sensor polling interval
CHURN_SENSOR_INTERVAL = int(os.getenv("CHURN_SENSOR_INTERVAL", "60"))

# Schedule config
CHURN_CRON = os.getenv("CHURN_CRON", "0 * * * *")


def get_conn():
    try:
        return psycopg2.connect(
            dbname=POSTGRES_DATA_WAREHOUSE,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
        )
    except psycopg2.OperationalError as e:
        raise RetryRequested(
            max_retries=DBT_MAX_RETRIES,
            seconds_to_wait=DBT_RETRY_WAIT_SECONDS,
            reason=f"Transient DB connection error: {e}",
        ) from e
    except psycopg2.Error as e:
        raise Failure(
            description="Failed to connect to warehouse database.",
            metadata={"error": str(e)},
        ) from e


def validate_columns(df: pd.DataFrame, required: Iterable[str]):
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise Failure(
            description="Input CSV missing required columns.",
            metadata={
                "missing_columns": ",".join(missing),
                "available_columns": ",".join(df.columns.astype(str)),
            },
        )
