"""Tests for the Railway PostgreSQL extraction module."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from ingestion.railway_extract import TABLES, extract_table


class TestTableConfig:
    """Test that table configurations are correct."""

    def test_has_four_tables(self):
        assert len(TABLES) == 4

    def test_table_names(self):
        names = {t["name"] for t in TABLES}
        assert names == {"transactions", "receipts", "users", "user_profiles"}

    def test_each_table_has_query(self):
        for table in TABLES:
            assert "query" in table
            assert "SELECT" in table["query"].upper()

    def test_receipts_filters_completed(self):
        receipts = next(t for t in TABLES if t["name"] == "receipts")
        assert "COMPLETED" in receipts["query"]


class TestExtractTable:
    """Test the extract_table function."""

    def test_returns_dataframe(self):
        mock_engine = MagicMock()
        mock_df = pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})

        with patch("ingestion.railway_extract.pd.read_sql", return_value=mock_df):
            result = extract_table(mock_engine, {"name": "test", "query": "SELECT 1"})

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3

    def test_empty_table_returns_empty_df(self):
        mock_engine = MagicMock()
        empty_df = pd.DataFrame()

        with patch("ingestion.railway_extract.pd.read_sql", return_value=empty_df):
            result = extract_table(mock_engine, {"name": "test", "query": "SELECT 1"})

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0
