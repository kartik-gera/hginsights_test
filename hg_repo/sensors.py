import os
from pathlib import Path

from dagster import Failure, RunRequest, SkipReason, sensor

from hg_repo.jobs import churn_pipeline_job
from hg_repo.config import CHURN_SENSOR_INTERVAL


@sensor(
    name="churn_file_sensor",
    job=churn_pipeline_job,
    minimum_interval_seconds=CHURN_SENSOR_INTERVAL,
)
def churn_file_sensor(context):
    """
    Sensor watches data/customer_churn.csv.
    Triggers churn_pipeline_job only when the file has a newer modtime
    than the last recorded cursor.
    """
    project_root = Path(__file__).resolve().parents[2]
    csv_rel_path = os.getenv("CSV_PATH", "data/customer_churn.csv")
    csv_path = project_root / csv_rel_path

    if not csv_path.exists():
        # Not a hard failure; just skip.
        return SkipReason(f"CSV not found at {csv_path}")

    try:
        mtime = csv_path.stat().st_mtime  # float timestamp
    except OSError as e:
        raise Failure(
            description="Failed to stat CSV file for sensor.",
            metadata={"path": str(csv_path), "error": str(e)},
        ) from e

    last_cursor = float(context.cursor) if context.cursor else 0.0

    if mtime <= last_cursor:
        return SkipReason("No new customer_churn.csv update detected")

    context.log.info(
        f"Detected new/updated CSV at {csv_path} "
        f"(mtime={mtime}, last_cursor={last_cursor})"
    )

    # Update cursor so we don't re-trigger for the same file
    context.update_cursor(str(mtime))

    yield RunRequest(
        run_key=f"churn_pipeline_{int(mtime)}",
        run_config={},
    )
