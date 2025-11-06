"""
Simplified Prefect flow for CFPB complaints pipeline.

Single job that:
1. Extracts data from START_DATE once (initial load)
2. Then appends data for each new day (incremental loads)
3. Works for a specific set of companies
4. Runs dbt models to transform the data
"""

import logging
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any

from prefect import flow, task

from ..cfg.config import COMPANIES, START_DATE
from ..pipelines.cfpb_complaints_pipeline import create_pipeline, extract_complaints
from ..utils.state import get_next_load_date, update_last_loaded_date

logger = logging.getLogger(__name__)


@task(name="extract_and_load_complaints", log_prints=True)
def extract_and_load_complaints_task(
    date_min: str,
    date_max: str,
    company_name: str,
    database_path: str = "database/cfpb_complaints.duckdb",
) -> dict[str, Any]:
    """
    Prefect task to extract and load complaints for a company.

    Args:
        date_min: Minimum received date (YYYY-MM-DD)
        date_max: Maximum received date (YYYY-MM-DD)
        company_name: Company name to filter
        database_path: Path to DuckDB database file

    Returns:
        Dictionary with execution results
    """
    logger.info(f"Loading complaints for {company_name}: {date_min} to {date_max}")

    pipeline = create_pipeline(database_path=database_path)

    info = pipeline.run(
        extract_complaints(
            date_received_min=date_min,
            date_received_max=date_max,
            company_name=company_name,
        )
    )

    logger.info(f"Completed loading for {company_name}")
    return {
        "company": company_name,
        "status": "success",
        "date_range": f"{date_min} to {date_max}",
        "info": str(info),
    }


@task(name="run_dbt_models", log_prints=True, retries=2, retry_delay_seconds=10)
def run_dbt_models_task() -> dict[str, Any]:
    """
    Prefect task to run all dbt models.

    Returns:
        Dictionary with execution results
    """
    logger.info("Running dbt models...")

    # Get the dbt project directory
    dbt_project_dir = Path(__file__).parent.parent.parent / "duckdb_dbt"

    if not dbt_project_dir.exists():
        error_msg = f"dbt project directory not found: {dbt_project_dir}"
        logger.error(error_msg)
        return {"status": "failed", "error": error_msg}

    try:
        # Run dbt models
        result = subprocess.run(
            ["dbt", "run"],
            cwd=str(dbt_project_dir),
            capture_output=True,
            text=True,
            check=True,
        )

        logger.info("dbt run completed successfully")
        logger.info(f"dbt output:\n{result.stdout}")

        return {
            "status": "success",
            "stdout": result.stdout,
            "stderr": result.stderr,
        }

    except subprocess.CalledProcessError as e:
        error_msg = f"dbt run failed with exit code {e.returncode}"
        logger.error(error_msg)
        logger.error(f"stdout:\n{e.stdout}")
        logger.error(f"stderr:\n{e.stderr}")
        return {
            "status": "failed",
            "error": error_msg,
            "stdout": e.stdout,
            "stderr": e.stderr,
        }
    except FileNotFoundError:
        error_msg = "dbt command not found. Make sure dbt is installed and in PATH."
        logger.error(error_msg)
        return {"status": "failed", "error": error_msg}
    except Exception as e:
        error_msg = f"Unexpected error running dbt: {str(e)}"
        logger.error(error_msg)
        return {"status": "failed", "error": error_msg}


@task(name="run_dbt_tests", log_prints=True, retries=2, retry_delay_seconds=10)
def run_dbt_tests_task() -> dict[str, Any]:
    """
    Prefect task to run dbt tests.

    Returns:
        Dictionary with execution results
    """
    logger.info("Running dbt tests...")

    # Get the dbt project directory
    dbt_project_dir = Path(__file__).parent.parent.parent / "duckdb_dbt"

    try:
        # Run dbt tests
        result = subprocess.run(
            ["dbt", "test"],
            cwd=str(dbt_project_dir),
            capture_output=True,
            text=True,
            check=True,
        )

        logger.info("dbt test completed successfully")
        logger.info(f"dbt test output:\n{result.stdout}")

        return {
            "status": "success",
            "stdout": result.stdout,
            "stderr": result.stderr,
        }

    except subprocess.CalledProcessError as e:
        error_msg = f"dbt test failed with exit code {e.returncode}"
        logger.warning(error_msg)
        logger.warning(f"stdout:\n{e.stdout}")
        logger.warning(f"stderr:\n{e.stderr}")
        # Don't fail the flow if tests fail, just warn
        return {
            "status": "warning",
            "error": error_msg,
            "stdout": e.stdout,
            "stderr": e.stderr,
        }
    except FileNotFoundError:
        error_msg = "dbt command not found. Make sure dbt is installed and in PATH."
        logger.error(error_msg)
        return {"status": "failed", "error": error_msg}
    except Exception as e:
        error_msg = f"Unexpected error running dbt tests: {str(e)}"
        logger.warning(error_msg)
        return {"status": "warning", "error": error_msg}


@flow(
    name="cfpb-complaints-incremental",
    description="Incremental load of CFPB complaints with dbt transformations",
    log_prints=True,
)
def cfpb_complaints_incremental_flow(
    database_path: str = "database/cfpb_complaints.duckdb",
) -> dict[str, Any]:
    """
    Single Prefect flow for incremental CFPB complaints loading and transformation.

    Pipeline steps:
    1. Extract & Load: Loads from START_DATE to today (first run) or only new days (incremental)
    2. Transform: Runs all dbt models (staging → intermediate → marts)
    3. Test: Runs dbt tests to validate data quality

    The flow processes all companies from config and updates state only on successful completion.

    Args:
        database_path: Path to DuckDB database file

    Returns:
        Dictionary with execution summary including dbt results
    """
    logger.info("Starting incremental CFPB complaints flow")

    # Determine date range for this run
    date_min, date_max = get_next_load_date(START_DATE)

    # Check if there's actually new data to load
    date_min_obj = datetime.strptime(date_min, "%Y-%m-%d")
    date_max_obj = datetime.strptime(date_max, "%Y-%m-%d")

    if date_min_obj > date_max_obj:
        logger.info("No new data to load (up to date)")
        return {
            "status": "skipped",
            "message": "Already up to date",
            "last_date": date_max,
        }

    logger.info(
        f"Loading data for {len(COMPANIES)} companies from {date_min} to {date_max}"
    )

    # Load data for each company
    results = []
    for company in COMPANIES:
        try:
            result = extract_and_load_complaints_task(
                date_min=date_min,
                date_max=date_max,
                company_name=company,
                database_path=database_path,
            )
            results.append(result)
        except Exception as e:
            logger.error(f"Failed to load data for {company}: {e}")
            results.append(
                {
                    "company": company,
                    "status": "failed",
                    "error": str(e),
                }
            )

    # Update state only if all companies succeeded
    successful = [r for r in results if r.get("status") == "success"]
    if len(successful) == len(COMPANIES):
        update_last_loaded_date(date_max)
        logger.info(f"State updated: last_loaded_date = {date_max}")
    else:
        logger.warning(
            f"Not all companies loaded successfully. "
            f"State not updated. ({len(successful)}/{len(COMPANIES)} successful)"
        )

    # Run dbt models to transform the loaded data
    dbt_result = None
    dbt_test_result = None
    if len(successful) > 0:
        logger.info("Running dbt transformations...")
        dbt_result = run_dbt_models_task()

        if dbt_result and dbt_result.get("status") == "success":
            logger.info("dbt models ran successfully, running tests...")
            dbt_test_result = run_dbt_tests_task()
        else:
            logger.error("dbt models failed, skipping tests")
    else:
        logger.warning("No data loaded successfully, skipping dbt transformations")

    summary = {
        "date_range": f"{date_min} to {date_max}",
        "total_companies": len(COMPANIES),
        "successful": len(successful),
        "failed": len([r for r in results if r.get("status") == "failed"]),
        "results": results,
        "dbt_run": dbt_result,
        "dbt_tests": dbt_test_result,
    }

    logger.info(
        f"Flow completed: {summary['successful']}/{summary['total_companies']} companies successful"
    )
    if dbt_result:
        logger.info(f"dbt run status: {dbt_result.get('status')}")
    if dbt_test_result:
        logger.info(f"dbt tests status: {dbt_test_result.get('status')}")

    return summary
