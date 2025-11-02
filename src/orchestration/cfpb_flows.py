"""
Simplified Prefect flow for CFPB complaints pipeline.

Single job that:
1. Extracts data from START_DATE once (initial load)
2. Then appends data for each new day (incremental loads)
3. Works for a specific set of companies
"""

import logging
from datetime import datetime
from typing import Any, Dict

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
) -> Dict[str, Any]:
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


@flow(
    name="cfpb-complaints-incremental",
    description="Incremental load of CFPB complaints for configured companies",
    log_prints=True,
)
def cfpb_complaints_incremental_flow(
    database_path: str = "database/cfpb_complaints.duckdb",
) -> Dict[str, Any]:
    """
    Single Prefect flow for incremental CFPB complaints loading.

    - First run: Loads from START_DATE to today
    - Subsequent runs: Loads only new days (incremental)
    - Processes all companies from config

    Args:
        database_path: Path to DuckDB database file

    Returns:
        Dictionary with execution summary
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

    summary = {
        "date_range": f"{date_min} to {date_max}",
        "total_companies": len(COMPANIES),
        "successful": len(successful),
        "failed": len([r for r in results if r.get("status") == "failed"]),
        "results": results,
    }

    logger.info(
        f"Flow completed: {summary['successful']}/{summary['total_companies']} companies successful"
    )
    return summary
