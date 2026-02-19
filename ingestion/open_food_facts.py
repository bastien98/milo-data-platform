"""
Seed product/brand catalog from the Open Food Facts API.

Downloads Belgian products with brand information and loads into Snowflake RAW
for later use in brand master data enrichment.
"""

import logging
import time

import httpx
import pandas as pd

from ingestion.config import OFF_API_BASE, OFF_COUNTRY, OFF_MAX_PAGES, OFF_PAGE_SIZE
from ingestion.snowflake_loader import load_dataframe

logger = logging.getLogger(__name__)

# Fields to extract from OFF API
OFF_FIELDS = [
    "code",
    "product_name",
    "brands",
    "categories_tags",
    "countries_tags",
    "stores",
    "quantity",
    "nutriscore_grade",
    "nova_group",
    "ecoscore_grade",
    "image_url",
]


def fetch_belgian_products(max_pages: int = OFF_MAX_PAGES) -> pd.DataFrame:
    """
    Fetch Belgian products from Open Food Facts API.

    Returns a DataFrame with one row per product.
    """
    all_products = []

    with httpx.Client(timeout=30) as client:
        for page in range(1, max_pages + 1):
            url = f"{OFF_API_BASE}/search"
            params = {
                "countries_tags_contains": f"en:{OFF_COUNTRY}",
                "fields": ",".join(OFF_FIELDS),
                "page_size": OFF_PAGE_SIZE,
                "page": page,
                "json": 1,
            }

            logger.info("Fetching OFF page %d/%d...", page, max_pages)
            resp = client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()

            products = data.get("products", [])
            if not products:
                logger.info("No more products at page %d, stopping.", page)
                break

            all_products.extend(products)
            logger.info("Fetched %d products (total: %d)", len(products), len(all_products))

            # Rate limiting: be polite to the API
            time.sleep(1)

    if not all_products:
        logger.warning("No products fetched from OFF API.")
        return pd.DataFrame()

    df = pd.DataFrame(all_products)

    # Clean up: extract primary brand
    if "brands" in df.columns:
        df["primary_brand"] = (
            df["brands"]
            .fillna("")
            .str.split(",")
            .str[0]
            .str.strip()
        )

    # Flatten categories_tags to first category
    if "categories_tags" in df.columns:
        df["primary_category"] = (
            df["categories_tags"]
            .apply(lambda x: x[0].replace("en:", "") if isinstance(x, list) and x else "")
        )

    logger.info("Total products fetched: %d", len(df))
    return df


def run(overwrite: bool = True):
    """Fetch Belgian products from OFF and load into Snowflake RAW."""
    df = fetch_belgian_products()
    if df.empty:
        logger.warning("No OFF data to load.")
        return

    # Select and rename columns for Snowflake
    columns_map = {
        "code": "barcode",
        "product_name": "product_name",
        "brands": "brands_raw",
        "primary_brand": "primary_brand",
        "primary_category": "off_category",
        "stores": "stores",
        "quantity": "quantity",
        "nutriscore_grade": "nutriscore",
        "nova_group": "nova_group",
        "ecoscore_grade": "ecoscore",
    }

    # Only include columns that exist
    available = {k: v for k, v in columns_map.items() if k in df.columns}
    df_clean = df[list(available.keys())].rename(columns=available)

    rows = load_dataframe(df_clean, table_name="off_products", overwrite=overwrite)
    logger.info("Loaded %d OFF products into RAW.OFF_PRODUCTS", rows)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
