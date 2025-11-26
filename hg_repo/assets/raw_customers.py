# hg_repo/assets_raw.py
import os
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dagster import (
    asset,
    AssetExecutionContext,
    Failure,
    MetadataValue,
)

from hg_repo.config import get_conn, validate_columns


@asset
def raw_customers(context: AssetExecutionContext):
    """Load CSV → raw.customers_churn"""

    project_root = Path(__file__).resolve().parents[2]
    csv_rel_path = os.getenv("CSV_PATH", "data/customer_churn.csv")
    csv_path = project_root / csv_rel_path

    if not csv_path.exists():
        raise Failure(
            description="Customer churn CSV not found.",
            metadata={"expected_path": str(csv_path)},
        )

    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        raise Failure(
            description="Failed to read customer churn CSV.",
            metadata={"path": str(csv_path), "error": str(e)},
        ) from e

    context.log.info(f"Loaded {len(df)} rows from {csv_path}")

    # Validate schema
    required_cols = [
        "CustomerID", "Age", "Gender", "Tenure", "MonthlyCharges",
        "ContractType", "InternetService", "TechSupport",
        "TotalCharges", "Churn",
    ]
    validate_columns(df, required_cols)

    # Normalise column names
    df.columns = [c.strip().replace(" ", "_") for c in df.columns]
    create_schema_sql = "CREATE SCHEMA IF NOT EXISTS raw;"
    create_table_sql = """
        CREATE TABLE IF NOT EXISTS raw.customers_churn (
            customer_id VARCHAR PRIMARY KEY,
            age INT,
            gender VARCHAR(10),
            tenure INT,
            monthly_charges NUMERIC(10,2),
            contract_type VARCHAR(50),
            internet_service VARCHAR(50),
            tech_support VARCHAR(10),
            total_charges NUMERIC(10,2),
            churn VARCHAR(10)
        );
    """

    insert_sql = """
        INSERT INTO raw.customers_churn (
            customer_id, age, gender, tenure, monthly_charges,
            contract_type, internet_service, tech_support,
            total_charges, churn
        )
        VALUES %s
        ON CONFLICT (customer_id) DO UPDATE SET
            age = EXCLUDED.age,
            gender = EXCLUDED.gender,
            tenure = EXCLUDED.tenure,
            monthly_charges = EXCLUDED.monthly_charges,
            contract_type = EXCLUDED.contract_type,
            internet_service = EXCLUDED.internet_service,
            tech_support = EXCLUDED.tech_support,
            total_charges = EXCLUDED.total_charges,
            churn = EXCLUDED.churn;
    """

    rows = []
    for _, row in df.iterrows():
        rows.append(
            (
                str(row["CustomerID"]),
                int(row["Age"]) if not pd.isna(row["Age"]) else None,
                row["Gender"],
                int(row["Tenure"]) if not pd.isna(row["Tenure"]) else None,
                float(row["MonthlyCharges"]) if not pd.isna(row["MonthlyCharges"]) else None,
                row["ContractType"],
                row["InternetService"],
                row["TechSupport"],
                float(row["TotalCharges"]) if not pd.isna(row["TotalCharges"]) else None,
                row["Churn"],
            )
        )

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(create_schema_sql)
                cur.execute(create_table_sql)
                cur.execute("TRUNCATE TABLE raw.customers_churn;")
                execute_values(cur, insert_sql, rows)
    except psycopg2.Error as e:
        raise Failure(
            description="Database error while writing raw.customers_churn.",
            metadata={"error": str(e)},
        ) from e

    context.add_output_metadata(
        {
            "row_count": MetadataValue.int(len(df)),
            "source_path": MetadataValue.path(str(csv_path)),
            "target_table": MetadataValue.text("raw.customers_churn"),
        }
    )

    return len(df)
