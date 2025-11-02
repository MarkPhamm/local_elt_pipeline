"""
State management for tracking pipeline execution.

Tracks the last successfully loaded date to enable incremental loads.
"""

import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

STATE_FILE = Path("pipeline_state.json")


def get_last_loaded_date() -> Optional[str]:
    """
    Get the last successfully loaded date from state file.

    Returns:
        Last loaded date (YYYY-MM-DD) or None if no previous load
    """
    if not STATE_FILE.exists():
        return None

    try:
        with open(STATE_FILE, "r") as f:
            state = json.load(f)
            return state.get("last_loaded_date")
    except (json.JSONDecodeError, IOError) as e:
        logger.warning(f"Error reading state file: {e}")
        return None


def update_last_loaded_date(date: str) -> None:
    """
    Update the last successfully loaded date in state file.

    Args:
        date: Date string (YYYY-MM-DD)
    """
    state = {
        "last_loaded_date": date,
        "updated_at": datetime.now().isoformat(),
    }

    try:
        with open(STATE_FILE, "w") as f:
            json.dump(state, f, indent=2)
        logger.info(f"Updated state: last_loaded_date = {date}")
    except IOError as e:
        logger.error(f"Error writing state file: {e}")
        raise


def reset_state() -> None:
    """Reset the state file (for manual re-initialization)."""
    if STATE_FILE.exists():
        STATE_FILE.unlink()
        logger.info("State file reset")


def get_next_load_date(start_date: str) -> tuple[str, str]:
    """
    Determine the date range for the next load.

    Args:
        start_date: Initial start date (YYYY-MM-DD)

    Returns:
        Tuple of (date_min, date_max) for the load
    """
    last_date = get_last_loaded_date()
    today = datetime.now().strftime("%Y-%m-%d")

    if last_date is None:
        # Initial load: load from start_date to today
        logger.info(f"Initial load: {start_date} to {today}")
        return start_date, today
    else:
        # Incremental load: load from day after last_date to today
        last_date_obj = datetime.strptime(last_date, "%Y-%m-%d")
        next_date_obj = last_date_obj + timedelta(days=1)
        next_date = next_date_obj.strftime("%Y-%m-%d")

        logger.info(f"Incremental load: {next_date} to {today}")
        return next_date, today
