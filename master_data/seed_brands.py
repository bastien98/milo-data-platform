"""
Build the initial seed_brand_lookup.csv from transaction data + Open Food Facts.

Extracts distinct normalized_brand values from transactions, cross-references
with OFF data, and identifies private label brands by matching against known
retailer brand patterns.

Usage:
    python -m master_data.seed_brands
"""

import logging

import pandas as pd

from ingestion.snowflake_loader import execute_query

logger = logging.getLogger(__name__)

# Patterns that identify private label brands
PRIVATE_LABEL_PATTERNS = {
    "Boni": "Colruyt Group",
    "Everyday": "Colruyt Group",
    "Ecoplus": "Colruyt Group",
    "Graindor": "Colruyt Group",
    "Okay": "Colruyt Group",
    "365": "Delhaize",
    "Delhaize": "Delhaize",
    "Bio Delhaize": "Delhaize",
    "Carrefour": "Carrefour Group",
    "Carrefour Bio": "Carrefour Group",
    "Carrefour Classic": "Carrefour Group",
    "Lidl": "Lidl",
    "Milbona": "Lidl",
    "Cien": "Lidl",
    "Formil": "Lidl",
    "W5": "Lidl",
    "Freeway": "Lidl",
    "Solevita": "Lidl",
    "Aldi": "Aldi",
    "Lashuma": "Aldi",
    "Moddys": "Aldi",
    "River": "Aldi",
}


def get_transaction_brands() -> pd.DataFrame:
    """Get distinct brands with transaction counts."""
    query = """
        SELECT
            NORMALIZED_BRAND AS brand_name,
            COUNT(*) AS transaction_count,
            COUNT(DISTINCT USER_ID) AS unique_buyers
        FROM RAW.TRANSACTIONS
        WHERE NORMALIZED_BRAND IS NOT NULL
          AND NORMALIZED_BRAND != ''
        GROUP BY NORMALIZED_BRAND
        ORDER BY transaction_count DESC
    """
    return execute_query(query)


def get_off_brands() -> pd.DataFrame:
    """Get brand info from Open Food Facts."""
    query = """
        SELECT DISTINCT
            PRIMARY_BRAND AS brand_name
        FROM RAW.OFF_PRODUCTS
        WHERE PRIMARY_BRAND IS NOT NULL
          AND PRIMARY_BRAND != ''
    """
    return execute_query(query)


def classify_brand(brand_name: str) -> tuple[bool, str]:
    """Check if a brand is private label and return (is_private_label, retailer_owner)."""
    for pattern, retailer in PRIVATE_LABEL_PATTERNS.items():
        if brand_name.lower() == pattern.lower():
            return True, retailer
    return False, ""


def run(output_path: str = "transform/seeds/seed_brand_lookup.csv"):
    """Build the brand lookup seed CSV."""
    # Get brands from transactions
    tx_brands = get_transaction_brands()
    logger.info("Found %d distinct brands in transactions", len(tx_brands))

    # Get brands from OFF
    off_brands = get_off_brands()
    logger.info("Found %d distinct brands in Open Food Facts", len(off_brands))

    # Combine and deduplicate
    all_brands = set(tx_brands["brand_name"].tolist())
    all_brands.update(off_brands["brand_name"].tolist())
    logger.info("Total unique brands: %d", len(all_brands))

    # Build lookup
    records = []
    for brand in sorted(all_brands):
        is_pl, retailer = classify_brand(brand)
        records.append({
            "brand_name": brand,
            "is_private_label": is_pl,
            "retailer_owner": retailer,
            "manufacturer": "",  # To be filled manually or via OFF
        })

    df = pd.DataFrame(records)
    df.to_csv(output_path, index=False)
    logger.info("Wrote %d brands to %s", len(df), output_path)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
