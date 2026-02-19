"""
Match store_name + store_branch from receipts to OSM store locations.

For each unique store_name + store_branch combination in the receipts,
find the best matching OSM location to enrich with lat/lng/city/province.

Matching strategy:
1. Exact match on store_name + city (from branch text)
2. Fuzzy match on branch text vs OSM branch/street/city
3. Manual override via seed_store_lookup.csv

Usage:
    python -m master_data.store_enricher
"""

import logging
from difflib import SequenceMatcher

import pandas as pd

from ingestion.snowflake_loader import execute_query, load_dataframe

logger = logging.getLogger(__name__)

MATCH_THRESHOLD = 0.75


def get_receipt_stores() -> pd.DataFrame:
    """Get distinct store_name + store_branch combinations from receipts."""
    query = """
        SELECT DISTINCT
            STORE_NAME,
            STORE_BRANCH
        FROM RAW.RECEIPTS
        WHERE STORE_NAME IS NOT NULL
          AND STORE_NAME != ''
        ORDER BY STORE_NAME, STORE_BRANCH
    """
    return execute_query(query)


def get_osm_stores() -> pd.DataFrame:
    """Get all OSM store locations from RAW."""
    query = """
        SELECT
            OSM_ID,
            STORE_NAME,
            BRANCH,
            LAT,
            LNG,
            STREET,
            HOUSENUMBER,
            POSTCODE,
            CITY,
            PROVINCE
        FROM RAW.OSM_STORES
        WHERE LAT IS NOT NULL
    """
    return execute_query(query)


def fuzzy_match_branch(
    receipt_store: str,
    receipt_branch: str,
    osm_df: pd.DataFrame,
) -> dict | None:
    """
    Find the best OSM match for a receipt store + branch.

    Tries matching the branch text against OSM city, branch, and street fields.
    """
    if not receipt_branch:
        return None

    # Filter OSM stores by store name (case-insensitive)
    candidates = osm_df[
        osm_df["STORE_NAME"].str.upper() == receipt_store.upper()
    ]

    if candidates.empty:
        return None

    branch_lower = receipt_branch.lower().strip()
    best_score = 0.0
    best_match = None

    for _, row in candidates.iterrows():
        # Compare against multiple OSM fields
        compare_texts = [
            str(row.get("CITY", "")).lower(),
            str(row.get("BRANCH", "")).lower(),
            str(row.get("STREET", "")).lower(),
            f"{row.get('CITY', '')} {row.get('STREET', '')}".lower(),
        ]

        for text in compare_texts:
            if not text.strip():
                continue
            score = SequenceMatcher(None, branch_lower, text).ratio()
            if score > best_score:
                best_score = score
                best_match = row

    if best_score >= MATCH_THRESHOLD and best_match is not None:
        return {
            "osm_id": best_match["OSM_ID"],
            "lat": best_match["LAT"],
            "lng": best_match["LNG"],
            "city": best_match.get("CITY", ""),
            "postcode": best_match.get("POSTCODE", ""),
            "province": best_match.get("PROVINCE", ""),
            "street": best_match.get("STREET", ""),
            "match_score": round(best_score, 3),
        }

    return None


def run():
    """Run the store enrichment pipeline."""
    receipt_stores = get_receipt_stores()
    osm_stores = get_osm_stores()

    logger.info(
        "Matching %d receipt store/branch combos against %d OSM locations...",
        len(receipt_stores), len(osm_stores),
    )

    results = []
    matched = 0

    for _, row in receipt_stores.iterrows():
        store_name = row["STORE_NAME"]
        store_branch = row.get("STORE_BRANCH", "")

        match = fuzzy_match_branch(store_name, store_branch, osm_stores)

        result = {
            "store_name": store_name,
            "store_branch": store_branch or "",
        }

        if match:
            result.update(match)
            matched += 1
        else:
            result.update({
                "osm_id": None,
                "lat": None,
                "lng": None,
                "city": "",
                "postcode": "",
                "province": "",
                "street": "",
                "match_score": 0.0,
            })

        results.append(result)

    df = pd.DataFrame(results)
    logger.info("Matched %d/%d store locations (%.0f%%)", matched, len(df), matched / max(len(df), 1) * 100)

    # Load enriched store data into Snowflake
    rows = load_dataframe(df, table_name="store_locations_enriched", overwrite=True)
    logger.info("Loaded %d enriched store locations into RAW.STORE_LOCATIONS_ENRICHED", rows)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
