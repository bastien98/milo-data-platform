"""
Build the initial seed_store_lookup.csv from receipt data + OSM data.

Extracts distinct store names from receipts, maps them to retailer groups,
and classifies store types.

Usage:
    python -m master_data.seed_stores
"""

import logging

import pandas as pd

from ingestion.snowflake_loader import execute_query

logger = logging.getLogger(__name__)

# Manual retailer group classification
RETAILER_GROUPS = {
    "Colruyt": ("Colruyt Group", "Supermarket", True),
    "OKay": ("Colruyt Group", "Compact Supermarket", True),
    "Bio-Planet": ("Colruyt Group", "Organic Supermarket", False),
    "Cru": ("Colruyt Group", "Premium Market", False),
    "Delhaize": ("Ahold Delhaize", "Supermarket", False),
    "AD Delhaize": ("Ahold Delhaize", "Franchise Supermarket", False),
    "Proxy Delhaize": ("Ahold Delhaize", "Neighbourhood Store", False),
    "Albert Heijn": ("Ahold Delhaize", "Supermarket", False),
    "Carrefour": ("Carrefour Group", "Hypermarket", False),
    "Carrefour Market": ("Carrefour Group", "Supermarket", False),
    "Carrefour Express": ("Carrefour Group", "Convenience Store", False),
    "Lidl": ("Lidl", "Discount Supermarket", True),
    "Aldi": ("Aldi", "Discount Supermarket", True),
    "Intermarché": ("Les Mousquetaires", "Supermarket", False),
    "Match": ("Louis Delhaize Group", "Supermarket", False),
    "Spar": ("Spar Group", "Neighbourhood Store", False),
}


def get_receipt_store_names() -> list[str]:
    """Get distinct store names from receipts."""
    query = """
        SELECT DISTINCT STORE_NAME
        FROM RAW.RECEIPTS
        WHERE STORE_NAME IS NOT NULL
          AND STORE_NAME != ''
        ORDER BY STORE_NAME
    """
    df = execute_query(query)
    return df["STORE_NAME"].tolist()


def run(output_path: str = "transform/seeds/seed_store_lookup.csv"):
    """Build the store lookup seed CSV."""
    store_names = get_receipt_store_names()
    logger.info("Found %d distinct store names in receipts", len(store_names))

    records = []
    for name in store_names:
        if name in RETAILER_GROUPS:
            group, store_type, is_disc = RETAILER_GROUPS[name]
        else:
            # Unknown store — flag for manual classification
            group = "Unknown"
            store_type = "Unknown"
            is_disc = False
            logger.warning("Unknown store: %s — needs manual classification", name)

        records.append({
            "store_name": name,
            "retailer_group": group,
            "store_type": store_type,
            "is_discounter": is_disc,
        })

    df = pd.DataFrame(records)
    df.to_csv(output_path, index=False)
    logger.info("Wrote %d stores to %s", len(df), output_path)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
