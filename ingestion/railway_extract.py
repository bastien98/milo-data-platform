"""
Extract data from the Railway PostgreSQL transactional database.

Extracts 4 tables:
  - transactions
  - receipts
  - users
  - user_profiles

Each table is extracted in full (small dataset) and loaded into Snowflake RAW schema.
"""

import logging

import pandas as pd
from sqlalchemy import create_engine

from ingestion.config import RAILWAY_DB_URL
from ingestion.snowflake_loader import load_dataframe

logger = logging.getLogger(__name__)

TABLES = [
    {
        "name": "transactions",
        "query": """
            SELECT
                id,
                user_id,
                receipt_id,
                store_name,
                item_name,
                item_price,
                quantity,
                unit_price,
                normalized_name,
                normalized_brand,
                is_premium,
                is_discount,
                is_deposit,
                granular_category,
                category,
                health_score,
                unit_of_measure,
                weight_or_volume,
                price_per_unit_measure,
                date,
                created_at,
                updated_at
            FROM transactions
        """,
    },
    {
        "name": "receipts",
        "query": """
            SELECT
                id,
                user_id,
                store_name,
                receipt_date,
                receipt_time,
                total_amount,
                payment_method,
                total_savings,
                store_branch,
                status,
                source,
                image_url,
                created_at,
                updated_at
            FROM receipts
            WHERE status = 'COMPLETED'
        """,
    },
    {
        "name": "users",
        "query": """
            SELECT
                id,
                firebase_uid,
                email,
                created_at,
                updated_at
            FROM users
        """,
    },
    {
        "name": "user_profiles",
        "query": """
            SELECT
                id,
                user_id,
                first_name,
                last_name,
                gender,
                created_at,
                updated_at
            FROM user_profiles
        """,
    },
]


def extract_table(engine, table_config: dict) -> pd.DataFrame:
    """Extract a single table from Railway PG."""
    name = table_config["name"]
    query = table_config["query"]

    logger.info("Extracting %s from Railway...", name)
    df = pd.read_sql(query, engine)
    logger.info("Extracted %d rows from %s", len(df), name)
    return df


def run(overwrite: bool = True):
    """
    Extract all tables from Railway and load into Snowflake RAW.

    Args:
        overwrite: If True, replace existing data (full refresh).
    """
    engine = create_engine(RAILWAY_DB_URL)

    for table_config in TABLES:
        name = table_config["name"]
        try:
            df = extract_table(engine, table_config)
            rows = load_dataframe(df, table_name=name, overwrite=overwrite)
            logger.info("Loaded %d rows into RAW.%s", rows, name.upper())
        except Exception:
            logger.exception("Failed to extract/load %s", name)
            raise

    engine.dispose()
    logger.info("Railway extraction complete.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
