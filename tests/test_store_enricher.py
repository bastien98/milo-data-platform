"""Tests for the store enrichment module."""

from unittest.mock import patch

import pandas as pd
import pytest

from master_data.store_enricher import MATCH_THRESHOLD, fuzzy_match_branch


class TestFuzzyMatchBranch:
    """Test the fuzzy matching logic for stores."""

    @pytest.fixture
    def osm_data(self):
        """Sample OSM store data."""
        return pd.DataFrame({
            "OSM_ID": [1, 2, 3, 4],
            "STORE_NAME": ["Colruyt", "Colruyt", "Delhaize", "Lidl"],
            "BRANCH": ["", "", "", ""],
            "LAT": [50.85, 50.90, 50.87, 50.83],
            "LNG": [4.35, 4.40, 4.37, 4.33],
            "STREET": ["Rue de la Loi", "Mechelsesteenweg", "Avenue Louise", "Naamsestraat"],
            "CITY": ["Bruxelles", "Antwerpen", "Bruxelles", "Leuven"],
            "POSTCODE": ["1000", "2000", "1050", "3000"],
            "PROVINCE": ["Bruxelles", "Antwerpen", "Bruxelles", "Vlaams-Brabant"],
        })

    def test_exact_city_match(self, osm_data):
        result = fuzzy_match_branch("Colruyt", "Bruxelles", osm_data)
        assert result is not None
        assert result["city"] == "Bruxelles"

    def test_no_match_wrong_store(self, osm_data):
        result = fuzzy_match_branch("Aldi", "Bruxelles", osm_data)
        assert result is None

    def test_no_match_empty_branch(self, osm_data):
        result = fuzzy_match_branch("Colruyt", "", osm_data)
        assert result is None

    def test_no_match_none_branch(self, osm_data):
        result = fuzzy_match_branch("Colruyt", None, osm_data)
        assert result is None

    def test_match_has_required_fields(self, osm_data):
        result = fuzzy_match_branch("Colruyt", "Antwerpen", osm_data)
        assert result is not None
        assert "osm_id" in result
        assert "lat" in result
        assert "lng" in result
        assert "city" in result
        assert "match_score" in result

    def test_case_insensitive_store_name(self, osm_data):
        result = fuzzy_match_branch("COLRUYT", "Bruxelles", osm_data)
        assert result is not None

    def test_match_score_above_threshold(self, osm_data):
        result = fuzzy_match_branch("Colruyt", "Antwerpen", osm_data)
        assert result is not None
        assert result["match_score"] >= MATCH_THRESHOLD


class TestMatchThreshold:
    """Test threshold configuration."""

    def test_threshold_is_reasonable(self):
        assert 0.5 <= MATCH_THRESHOLD <= 1.0
