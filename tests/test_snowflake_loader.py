"""Tests for the Snowflake loader module."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


class TestLoadDataframe:
    """Test the load_dataframe function."""

    @patch("ingestion.snowflake_loader.get_connection")
    @patch("ingestion.snowflake_loader.write_pandas")
    def test_empty_df_skips_load(self, mock_write, mock_conn):
        from ingestion.snowflake_loader import load_dataframe

        result = load_dataframe(pd.DataFrame(), "test_table")
        assert result == 0
        mock_write.assert_not_called()

    @patch("ingestion.snowflake_loader.get_connection")
    @patch("ingestion.snowflake_loader.write_pandas")
    def test_uppercases_table_name(self, mock_write, mock_conn):
        from ingestion.snowflake_loader import load_dataframe

        mock_conn.return_value = MagicMock()
        mock_write.return_value = (True, 1, 5, None)

        df = pd.DataFrame({"col_a": [1, 2, 3], "col_b": ["x", "y", "z"]})
        load_dataframe(df, "my_table")

        # Check that write_pandas was called with uppercased table name
        call_args = mock_write.call_args
        assert call_args[0][2] == "MY_TABLE"  # table_name arg

    @patch("ingestion.snowflake_loader.get_connection")
    @patch("ingestion.snowflake_loader.write_pandas")
    def test_uppercases_column_names(self, mock_write, mock_conn):
        from ingestion.snowflake_loader import load_dataframe

        mock_conn.return_value = MagicMock()
        mock_write.return_value = (True, 1, 3, None)

        df = pd.DataFrame({"lower_col": [1, 2, 3]})
        load_dataframe(df, "test")

        # Check the DataFrame passed to write_pandas has uppercased columns
        written_df = mock_write.call_args[0][1]
        assert list(written_df.columns) == ["LOWER_COL"]

    @patch("ingestion.snowflake_loader.get_connection")
    @patch("ingestion.snowflake_loader.write_pandas")
    def test_returns_row_count(self, mock_write, mock_conn):
        from ingestion.snowflake_loader import load_dataframe

        mock_conn.return_value = MagicMock()
        mock_write.return_value = (True, 1, 42, None)

        df = pd.DataFrame({"a": range(42)})
        result = load_dataframe(df, "test")
        assert result == 42

    @patch("ingestion.snowflake_loader.get_connection")
    @patch("ingestion.snowflake_loader.write_pandas")
    def test_overwrite_truncates_first(self, mock_write, mock_conn):
        from ingestion.snowflake_loader import load_dataframe

        mock_cursor = MagicMock()
        mock_connection = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_conn.return_value = mock_connection
        mock_write.return_value = (True, 1, 5, None)

        df = pd.DataFrame({"a": [1, 2, 3]})
        load_dataframe(df, "test", overwrite=True)

        # Check that TRUNCATE was called
        calls = [str(c) for c in mock_cursor.execute.call_args_list]
        truncate_called = any("TRUNCATE" in c for c in calls)
        assert truncate_called
