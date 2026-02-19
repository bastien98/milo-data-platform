"""Tests for the brand matching module."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from master_data.brand_matcher import CONFIDENCE_THRESHOLD, match_brands


class TestMatchBrands:
    """Test brand matching logic."""

    def test_empty_input_returns_empty_df(self):
        model = MagicMock()
        index = MagicMock()

        result = match_brands([], model, index)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0
        assert "input_brand" in result.columns

    def test_confident_match_above_threshold(self):
        model = MagicMock()
        model.encode.return_value = [[0.1] * 384]

        # Mock Pinecone response
        mock_match = MagicMock()
        mock_match.score = 0.95
        mock_match.metadata = {
            "brand_name": "Jupiler",
            "is_private_label": False,
            "retailer_owner": "",
            "manufacturer": "AB InBev",
        }

        mock_response = MagicMock()
        mock_response.matches = [mock_match]

        index = MagicMock()
        index.query.return_value = mock_response

        result = match_brands(["Jupiler Pils"], model, index)

        assert len(result) == 1
        assert result.iloc[0]["matched_brand"] == "Jupiler"
        assert result.iloc[0]["is_confident"] is True
        assert result.iloc[0]["similarity"] == 0.95

    def test_low_confidence_match(self):
        model = MagicMock()
        model.encode.return_value = [[0.1] * 384]

        mock_match = MagicMock()
        mock_match.score = 0.60  # Below threshold
        mock_match.metadata = {
            "brand_name": "Unknown Brand",
            "is_private_label": False,
            "retailer_owner": "",
            "manufacturer": "",
        }

        mock_response = MagicMock()
        mock_response.matches = [mock_match]

        index = MagicMock()
        index.query.return_value = mock_response

        result = match_brands(["Some Random Brand"], model, index)

        assert len(result) == 1
        assert result.iloc[0]["is_confident"] is False

    def test_no_matches_returns_empty_match(self):
        model = MagicMock()
        model.encode.return_value = [[0.1] * 384]

        mock_response = MagicMock()
        mock_response.matches = []

        index = MagicMock()
        index.query.return_value = mock_response

        result = match_brands(["Nonexistent Brand"], model, index)

        assert len(result) == 1
        assert result.iloc[0]["matched_brand"] == ""
        assert result.iloc[0]["similarity"] == 0.0
        assert result.iloc[0]["is_confident"] is False


class TestConfidenceThreshold:
    """Test confidence threshold configuration."""

    def test_threshold_is_reasonable(self):
        assert 0.5 <= CONFIDENCE_THRESHOLD <= 1.0

    def test_threshold_is_85_percent(self):
        assert CONFIDENCE_THRESHOLD == 0.85
