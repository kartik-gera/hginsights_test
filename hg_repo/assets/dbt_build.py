# hg_repo/assets_dbt.py
import os
import subprocess
from pathlib import Path

from dagster import (
    asset,
    AssetExecutionContext,
    Failure,
    MetadataValue,
    RetryRequested,
)

from hg_repo.config import DBT_MAX_RETRIES, DBT_RETRY_WAIT_SECONDS
from hg_repo.assets.raw_customers import raw_customers


@asset(deps=[raw_customers])
def dbt_build(context: AssetExecutionContext):
    """
    Run dbt build for the churn_dbt project.

    Builds:
    - stging tables 
    - analytics tables
    + runs tests from schema.yml
    """

    project_root = Path(__file__).resolve().parents[2]
    dbt_project_dir = Path(
        os.getenv("DBT_PROJECT_DIR", str(project_root / "churn_dbt"))
    )

    if not dbt_project_dir.exists():
        raise Failure(
            description="dbt project directory not found.",
            metadata={"dbt_project_dir": str(dbt_project_dir)},
        )

    cmd = [
        "dbt",
        "build",
        "--project-dir",
        str(dbt_project_dir),
        "--profiles-dir",
        str(dbt_project_dir),
    ]

    context.log.info(f"Running dbt command: {' '.join(cmd)}")

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
        )
    except FileNotFoundError as e:
        raise Failure(
            description="dbt CLI not found in container PATH.",
            metadata={"error": str(e), "cmd": " ".join(cmd)},
        ) from e
    except Exception as e:
        raise Failure(
            description="Unexpected error while running dbt build.",
            metadata={"error": str(e)},
        ) from e

    stdout_tail = result.stdout[-2000:] if result.stdout else ""
    stderr_tail = result.stderr[-2000:] if result.stderr else ""

    context.log.info(f"dbt stdout (tail):\n{stdout_tail}")
    if stderr_tail:
        context.log.warning(f"dbt stderr (tail):\n{stderr_tail}")

    if result.returncode != 0:
        combined = f"{stdout_tail}\n{stderr_tail}".lower()

        if "could not connect to server" in combined or "connection refused" in combined:
            raise RetryRequested(
                max_retries=DBT_MAX_RETRIES,
                seconds_to_wait=DBT_RETRY_WAIT_SECONDS,
                reason="Transient DB issue during dbt build.",
            )

        raise Failure(
            description="dbt build failed.",
            metadata={
                "return_code": result.returncode,
                "stdout_tail": stdout_tail,
                "stderr_tail": stderr_tail,
            },
        )

    context.add_output_metadata(
        {
            "return_code": MetadataValue.int(result.returncode),
            "stdout_tail": MetadataValue.text(stdout_tail),
        }
    )

    context.log.info("dbt build completed successfully")
    return result.returncode
