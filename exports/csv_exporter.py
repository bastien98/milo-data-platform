"""
Export mart tables to CSV for client delivery.

Each client gets a filtered CSV based on their subscribed categories/stores.
Files are saved to exports/output/ with client name and date in the filename.
"""

import logging
import os
from datetime import datetime

import pandas as pd

from ingestion.snowflake_loader import execute_query

logger = logging.getLogger(__name__)

OUTPUT_DIR = "exports/output"


def ensure_output_dir():
    """Create output directory if it doesn't exist."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)


def export_category_performance(
    client_name: str,
    categories: list[str] | None = None,
    stores: list[str] | None = None,
    year_months: list[str] | None = None,
) -> str:
    """
    Export mart_category_performance to CSV with optional filters.

    Args:
        client_name: Client identifier for the filename.
        categories: Filter to specific parent_category values.
        stores: Filter to specific store_name values.
        year_months: Filter to specific months (YYYY-MM format).

    Returns:
        Path to the exported CSV file.
    """
    ensure_output_dir()

    query = "SELECT * FROM SCANDALICIOUS_DW.MARTS.MART_CATEGORY_PERFORMANCE WHERE 1=1"
    params = {}

    if categories:
        placeholders = ", ".join(f"'{c}'" for c in categories)
        query += f" AND PARENT_CATEGORY IN ({placeholders})"

    if stores:
        placeholders = ", ".join(f"'{s}'" for s in stores)
        query += f" AND STORE_NAME IN ({placeholders})"

    if year_months:
        placeholders = ", ".join(f"'{m}'" for m in year_months)
        query += f" AND YEAR_MONTH IN ({placeholders})"

    query += " ORDER BY YEAR_MONTH, GRANULAR_CATEGORY, STORE_NAME, BRAND_NAME"

    logger.info("Exporting category performance for client '%s'...", client_name)
    df = execute_query(query)

    timestamp = datetime.now().strftime("%Y%m%d")
    filename = f"{client_name}_category_performance_{timestamp}.csv"
    filepath = os.path.join(OUTPUT_DIR, filename)

    df.to_csv(filepath, index=False)
    logger.info("Exported %d rows to %s", len(df), filepath)
    return filepath


def export_panel_summary() -> str:
    """Export the panel summary for internal monitoring."""
    ensure_output_dir()

    query = "SELECT * FROM SCANDALICIOUS_DW.MARTS.MART_PANEL_SUMMARY ORDER BY YEAR_MONTH"
    df = execute_query(query)

    timestamp = datetime.now().strftime("%Y%m%d")
    filepath = os.path.join(OUTPUT_DIR, f"panel_summary_{timestamp}.csv")

    df.to_csv(filepath, index=False)
    logger.info("Exported panel summary (%d months) to %s", len(df), filepath)
    return filepath


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    # Example: export all data for a client
    export_category_performance("demo_client")
    export_panel_summary()
