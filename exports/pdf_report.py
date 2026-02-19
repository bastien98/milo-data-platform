"""
Generate branded PDF reports from mart data.

Uses Jinja2 templates and WeasyPrint for PDF rendering.
Each report covers a specific category × time period for a client.
"""

import logging
import os
from datetime import datetime

import pandas as pd
from jinja2 import Environment, FileSystemLoader
from weasyprint import HTML

from ingestion.snowflake_loader import execute_query

logger = logging.getLogger(__name__)

TEMPLATE_DIR = "exports/templates"
OUTPUT_DIR = "exports/output"


def ensure_output_dir():
    """Create output directory if it doesn't exist."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)


def get_category_data(parent_category: str, year_month: str) -> pd.DataFrame:
    """Fetch category performance data for a specific category and month."""
    query = f"""
        SELECT *
        FROM SCANDALICIOUS_DW.MARTS.MART_CATEGORY_PERFORMANCE
        WHERE PARENT_CATEGORY = '{parent_category}'
          AND YEAR_MONTH = '{year_month}'
        ORDER BY TOTAL_SPEND DESC
    """
    return execute_query(query)


def generate_report(
    client_name: str,
    parent_category: str,
    year_month: str,
) -> str:
    """
    Generate a PDF report for a specific category and month.

    Args:
        client_name: Client name for branding.
        parent_category: Category to report on.
        year_month: Month to report on (YYYY-MM).

    Returns:
        Path to the generated PDF.
    """
    ensure_output_dir()

    # Fetch data
    df = get_category_data(parent_category, year_month)
    if df.empty:
        logger.warning("No data for %s in %s", parent_category, year_month)
        return ""

    # Compute summary stats
    summary = {
        "total_spend": df["TOTAL_SPEND"].sum(),
        "unique_brands": df["BRAND_NAME"].nunique(),
        "unique_stores": df["STORE_NAME"].nunique(),
        "avg_penetration": df["PENETRATION_PCT"].mean(),
        "top_brand": df.groupby("BRAND_NAME")["TOTAL_SPEND"].sum().idxmax(),
        "top_store": df.groupby("STORE_NAME")["TOTAL_SPEND"].sum().idxmax(),
    }

    # Top brands by spend
    top_brands = (
        df.groupby("BRAND_NAME")["TOTAL_SPEND"]
        .sum()
        .sort_values(ascending=False)
        .head(10)
        .reset_index()
    )

    # Top stores by spend
    top_stores = (
        df.groupby("STORE_NAME")["TOTAL_SPEND"]
        .sum()
        .sort_values(ascending=False)
        .head(10)
        .reset_index()
    )

    # Render template
    env = Environment(loader=FileSystemLoader(TEMPLATE_DIR))
    template = env.get_template("category_report.html")

    html_content = template.render(
        client_name=client_name,
        category=parent_category,
        year_month=year_month,
        generated_at=datetime.now().strftime("%Y-%m-%d %H:%M"),
        summary=summary,
        top_brands=top_brands.to_dict("records"),
        top_stores=top_stores.to_dict("records"),
    )

    # Generate PDF
    timestamp = datetime.now().strftime("%Y%m%d")
    safe_category = parent_category.replace(" ", "_").replace("&", "and")
    filename = f"{client_name}_{safe_category}_{year_month}_{timestamp}.pdf"
    filepath = os.path.join(OUTPUT_DIR, filename)

    HTML(string=html_content).write_pdf(filepath)
    logger.info("Generated report: %s", filepath)
    return filepath


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    generate_report("demo_client", "Dairy, Eggs & Cheese", "2025-01")
