"""
Generic Snowflake loader: write Pandas DataFrames to Snowflake RAW schema.

Uses COPY INTO via write_pandas for efficient bulk loading.
"""

import logging

import pandas as pd
from snowflake.connector import connect
from snowflake.connector.pandas_tools import write_pandas

from ingestion.config import SNOWFLAKE_CONFIG, SNOWFLAKE_RAW_SCHEMA

logger = logging.getLogger(__name__)


def get_connection():
    """Create a Snowflake connection using config."""
    return connect(**SNOWFLAKE_CONFIG)


def load_dataframe(
    df: pd.DataFrame,
    table_name: str,
    schema: str = SNOWFLAKE_RAW_SCHEMA,
    overwrite: bool = False,
) -> int:
    """
    Load a DataFrame into a Snowflake table.

    Args:
        df: DataFrame to load.
        table_name: Target table name (will be uppercased).
        schema: Target schema (default: RAW).
        overwrite: If True, truncate before loading. If False, append.

    Returns:
        Number of rows loaded.
    """
    if df.empty:
        logger.warning("Empty DataFrame — skipping load for %s.%s", schema, table_name)
        return 0

    table_name = table_name.upper()
    schema = schema.upper()

    # Uppercase column names for Snowflake compatibility
    df.columns = [col.upper() for col in df.columns]

    conn = get_connection()
    try:
        conn.cursor().execute(f"USE SCHEMA {SNOWFLAKE_CONFIG['database']}.{schema}")

        if overwrite:
            logger.info("Truncating %s.%s before load", schema, table_name)
            conn.cursor().execute(f"TRUNCATE TABLE IF EXISTS {table_name}")

        success, num_chunks, num_rows, _ = write_pandas(
            conn,
            df,
            table_name,
            schema=schema,
            database=SNOWFLAKE_CONFIG["database"],
            auto_create_table=True,
            overwrite=False,  # We handle truncation manually above
        )

        if success:
            logger.info(
                "Loaded %d rows into %s.%s (%d chunks)",
                num_rows, schema, table_name, num_chunks,
            )
        else:
            logger.error("Failed to load data into %s.%s", schema, table_name)

        return num_rows
    finally:
        conn.close()


def execute_query(query: str, params: dict | None = None) -> pd.DataFrame:
    """Execute a query and return results as a DataFrame."""
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(query, params)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        return pd.DataFrame(rows, columns=columns)
    finally:
        conn.close()
