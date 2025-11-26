import psycopg2
from dagster import (
    asset,
    AssetExecutionContext,
    Failure,
    MetadataValue,
)

from hg_repo.config import get_conn
from hg_repo.assets.dbt_build import dbt_build


@asset(deps=[dbt_build])
def analytics_freshness_check(context: AssetExecutionContext):
    """
    Data freshness / health check for analytics layer.

    Reads public_analytics.churn_summary_by_contract and exposes:
    - row_count
    - checked_at (DB server time)
    """
    table_name = "public_analytics.churn_summary_by_contract"

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT
                        COUNT(*) AS row_count,
                        NOW()::timestamptz AS checked_at
                    FROM {table_name};
                    """
                )
                row = cur.fetchone()
    except psycopg2.Error as e:
        raise Failure(
            description="Failed to run freshness check on analytics table.",
            metadata={"table": table_name, "error": str(e)},
        ) from e

    row_count, checked_at = row

    context.log.info(
        f"Freshness check for {table_name}: row_count={row_count}, checked_at={checked_at}"
    )

    context.add_output_metadata(
        {
            "row_count": MetadataValue.int(row_count),
            "checked_at": MetadataValue.text(str(checked_at)),
            "table": MetadataValue.text(table_name),
        }
    )

    return row_count
