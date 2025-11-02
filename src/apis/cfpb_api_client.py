"""
CFPB Consumer Complaint Database API Client

This module provides a client for interacting with the Consumer Financial
Protection Bureau's Consumer Complaint Database API.

API Documentation: https://cfpb.github.io/api/ccdb/api.html
"""

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


class CFPBAPIClient:
    """Client for accessing the CFPB Consumer Complaint Database API."""

    BASE_URL = "https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/"
    DEFAULT_TIMEOUT = 30
    MAX_RETRIES = 3

    def __init__(self, timeout: int = DEFAULT_TIMEOUT):
        """
        Initialize the CFPB API client.

        Args:
            timeout: Request timeout in seconds
        """
        self.timeout = timeout
        self.session = self._create_session()

    def _create_session(self) -> requests.Session:
        """
        Create a requests session with retry logic and proper headers.

        Returns:
            Configured requests session
        """
        session = requests.Session()

        # CRITICAL: Add User-Agent header (CFPB API requires this to avoid 403 errors)
        session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (compatible; ConsumerComplaintETL/1.0; Python/requests)",
                "Accept": "application/json",
            }
        )

        # Configure retry strategy
        retry_strategy = Retry(
            total=self.MAX_RETRIES,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)

        return session

    def get_complaints(
        self,
        date_received_min: Optional[str] = None,
        date_received_max: Optional[str] = None,
        size: int = 10000,
        frm: int = 0,
        sort: str = "created_date_desc",
        fields: Optional[List[str]] = None,
        search_term: Optional[str] = None,
        search_field: Optional[str] = None,
        no_aggs: bool = False,
        **filters,
    ) -> Dict[str, Any]:
        """
        Fetch consumer complaints from the CFPB API.

        Args:
            date_received_min: Minimum received date (YYYY-MM-DD)
            date_received_max: Maximum received date (YYYY-MM-DD)
            size: Number of records to return (max 10000)
            frm: Starting index for pagination
            sort: Sort order for results
            fields: List of specific fields to return
            search_term: Search term to filter results (e.g., company name)
            search_field: Field to search in (e.g., 'company')
            no_aggs: Disable aggregations for faster responses
            **filters: Additional filter parameters (product, company, state, etc.)

        Returns:
            API response as dictionary

        Raises:
            requests.RequestException: If API request fails
        """
        params = {
            "size": min(size, 10000),  # API limit is 10000
            "frm": frm,
            "sort": sort,
            "format": "json",
        }

        # Add date filters
        if date_received_min:
            params["date_received_min"] = date_received_min
        if date_received_max:
            params["date_received_max"] = date_received_max

        # Add field selection
        if fields:
            params["field"] = fields

        # Add search parameters
        if search_term:
            params["search_term"] = search_term
        if search_field:
            params["field"] = search_field

        # Add no_aggs flag
        if no_aggs:
            params["no_aggs"] = "true"

        # Add additional filters
        params.update(filters)

        try:
            logger.info(f"Fetching complaints from CFPB API with params: {params}")
            response = self.session.get(
                self.BASE_URL, params=params, timeout=self.timeout
            )
            response.raise_for_status()

            data = response.json()

            # Handle different response formats
            if isinstance(data, list):
                # Direct list format
                logger.info(
                    f"Successfully fetched {len(data)} complaints (direct list format)"
                )
            elif isinstance(data, dict) and "hits" in data:
                # Nested dict format
                hits = data.get("hits", {}).get("hits", [])
                total_value = data.get("hits", {}).get("total", {})
                if isinstance(total_value, dict):
                    total = total_value.get("value", 0)
                else:
                    total = total_value
                logger.info(
                    f"Successfully fetched {len(hits)} complaints. Total available: {total}"
                )
            else:
                logger.warning(f"Unexpected response format: {type(data)}")

            return data

        except requests.RequestException as e:
            logger.error(f"Error fetching complaints from CFPB API: {e}")
            raise

    def get_complaints_paginated(
        self,
        date_received_min: Optional[str] = None,
        date_received_max: Optional[str] = None,
        max_records: Optional[int] = None,
        **filters,
    ) -> List[Dict[str, Any]]:
        """
        Fetch all complaints with pagination support.

        Args:
            date_received_min: Minimum received date (YYYY-MM-DD)
            date_received_max: Maximum received date (YYYY-MM-DD)
            max_records: Maximum total records to fetch (None for all)
            **filters: Additional filter parameters

        Returns:
            List of complaint records
        """
        all_complaints = []
        frm = 0
        page_size = 10000

        while True:
            # Check if we've reached max_records
            if max_records and len(all_complaints) >= max_records:
                logger.info(f"Reached max_records limit: {max_records}")
                break

            # Fetch page
            response = self.get_complaints(
                date_received_min=date_received_min,
                date_received_max=date_received_max,
                size=page_size,
                frm=frm,
                **filters,
            )

            # Handle different response formats
            if isinstance(response, list):
                # Direct list format
                hits = response
                complaints = [hit.get("_source", {}) for hit in hits]
                total_available = len(hits)
            elif isinstance(response, dict) and "hits" in response:
                # Nested dict format
                hits = response.get("hits", {}).get("hits", [])
                complaints = [hit.get("_source", {}) for hit in hits]
                total_value = response.get("hits", {}).get("total", {})
                if isinstance(total_value, dict):
                    total_available = total_value.get("value", 0)
                else:
                    total_available = total_value
            else:
                logger.warning("Unexpected response format")
                break

            if not hits:
                logger.info("No more complaints to fetch")
                break

            all_complaints.extend(complaints)

            logger.info(
                f"Fetched {len(complaints)} complaints. Total so far: {len(all_complaints)}"
            )

            # Check if there are more results
            if len(all_complaints) >= total_available:
                logger.info("Fetched all available complaints")
                break

            # Move to next page
            frm += page_size

        logger.info(f"Total complaints fetched: {len(all_complaints)}")
        return all_complaints

    def get_complaints_for_date_range(
        self, start_date: datetime, end_date: datetime, **filters
    ) -> List[Dict[str, Any]]:
        """
        Fetch complaints for a specific date range.

        Args:
            start_date: Start date for complaints
            end_date: End date for complaints
            **filters: Additional filter parameters

        Returns:
            List of complaint records
        """
        date_min = start_date.strftime("%Y-%m-%d")
        date_max = end_date.strftime("%Y-%m-%d")

        logger.info(f"Fetching complaints from {date_min} to {date_max}")

        return self.get_complaints_paginated(
            date_received_min=date_min, date_received_max=date_max, **filters
        )

    def get_complaints_last_n_days(
        self, days: int = 1, **filters
    ) -> List[Dict[str, Any]]:
        """
        Fetch complaints from the last N days.

        Args:
            days: Number of days to look back
            **filters: Additional filter parameters

        Returns:
            List of complaint records
        """
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)

        return self.get_complaints_for_date_range(
            start_date=start_date, end_date=end_date, **filters
        )

    def get_complaints_by_company(
        self,
        company_name: str,
        date_received_min: Optional[str] = None,
        date_received_max: Optional[str] = None,
        max_records: Optional[int] = None,
        no_aggs: bool = True,
        **filters,
    ) -> List[Dict[str, Any]]:
        """
        Fetch complaints for a specific company.

        Args:
            company_name: Name of the company to search for (e.g., 'jpmorgan')
            date_received_min: Minimum received date (YYYY-MM-DD)
            date_received_max: Maximum received date (YYYY-MM-DD)
            max_records: Maximum total records to fetch (None for all)
            no_aggs: Disable aggregations for faster responses (default: True)
            **filters: Additional filter parameters

        Returns:
            List of complaint records for the specified company

        Example:
            >>> client = CFPBAPIClient()
            >>> complaints = client.get_complaints_by_company(
            ...     company_name='jpmorgan',
            ...     date_received_min='2011-12-01',
            ...     date_received_max='2025-10-02'
            ... )
        """
        logger.info(f"Fetching complaints for company: {company_name}")

        return self.get_complaints_paginated(
            date_received_min=date_received_min,
            date_received_max=date_received_max,
            max_records=max_records,
            search_term=company_name,
            search_field="company",
            no_aggs=no_aggs,
            **filters,
        )

    def close(self):
        """Close the API client session."""
        if self.session:
            self.session.close()
            logger.info("CFPB API client session closed")
