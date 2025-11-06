"""
dlt Pipeline for CFPB Consumer Complaints

This pipeline extracts consumer complaint data from the CFPB API
and loads it into DuckDB in the raw schema.
"""

import hashlib
import logging
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path
from typing import Any

import dlt
from dlt.destinations import duckdb

from ..apis.cfpb_api_client import CFPBAPIClient

logger = logging.getLogger(__name__)


@dlt.resource(
    name="cfpb_complaints",
    write_disposition="merge",
    primary_key="complaint_id",
)
def extract_complaints(
    date_received_min: str | None = None,
    date_received_max: str | None = None,
    company_name: str | None = None,
    max_records: int | None = None,
) -> Iterator[dict[str, Any]]:
    """
    Extract complaints from CFPB API.

    Args:
        date_received_min: Minimum received date (YYYY-MM-DD)
        date_received_max: Maximum received date (YYYY-MM-DD)
        company_name: Optional company name filter
        max_records: Maximum number of records to fetch

    Yields:
        Complaint records from the API
    """
    client = CFPBAPIClient()

    try:
        if company_name:
            logger.info(f"Extracting complaints for company: {company_name}")
            complaints = client.get_complaints_by_company(
                company_name=company_name,
                date_received_min=date_received_min,
                date_received_max=date_received_max,
                max_records=max_records,
            )
        else:
            logger.info("Extracting all complaints")
            complaints = client.get_complaints_paginated(
                date_received_min=date_received_min,
                date_received_max=date_received_max,
                max_records=max_records,
            )

        # Add extraction metadata
        extraction_timestamp = datetime.now().isoformat()

        for complaint in complaints:
            # Ensure complaint_id exists for primary key
            # The API may return _id or id fields - use those to construct complaint_id
            if "complaint_id" not in complaint or not complaint.get("complaint_id"):
                # Try to get ID from various possible fields (API returns _source with _id)
                complaint_id_field = (
                    complaint.get("_id")
                    or complaint.get("id")
                    or complaint.get("complaint_id")
                )

                # If we have an ID, use it; otherwise construct from date + hash
                if complaint_id_field:
                    complaint["complaint_id"] = str(complaint_id_field)
                else:
                    # Fallback: use date_received + hash of complaint data
                    date_received = complaint.get("date_received", "")
                    complaint_str = str(sorted(complaint.items()))
                    complaint_hash = hashlib.md5(complaint_str.encode()).hexdigest()[:8]
                    complaint["complaint_id"] = f"{date_received}_{complaint_hash}"

            # Add extraction metadata
            complaint["_dlt_extracted_at"] = extraction_timestamp

            yield complaint

    finally:
        client.close()


def create_pipeline(
    database_path: str = "database/cfpb_complaints.duckdb",
    schema_name: str = "raw",
) -> dlt.Pipeline:
    """
    Create and configure the dlt pipeline for CFPB complaints.

    Args:
        database_path: Path to DuckDB database file
        schema_name: Schema name for the data (default: raw)

    Returns:
        Configured dlt pipeline
    """
    # Ensure database directory exists
    db_path = Path(database_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    # Create pipeline
    # DuckDB destination accepts database path as string or in credentials dict
    pipeline = dlt.pipeline(
        pipeline_name="cfpb_complaints",
        destination=duckdb(credentials=str(db_path.absolute())),
        dataset_name=schema_name,
    )

    return pipeline
