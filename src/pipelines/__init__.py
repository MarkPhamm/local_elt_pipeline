"""
dlt Pipelines for extracting and loading data.
"""

from .cfpb_complaints_pipeline import (
    create_pipeline,
    extract_complaints,
)

__all__ = [
    "create_pipeline",
    "extract_complaints",
]
