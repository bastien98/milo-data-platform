"""
Query Belgian grocery store locations from OpenStreetMap via the Overpass API.

Extracts store name, branch, lat/lng, city, province, postcode for all major
Belgian grocery chains. Results are loaded into Snowflake RAW.
"""

import logging
import time

import httpx
import pandas as pd

from ingestion.config import OSM_BELGIUM_BBOX, OSM_OVERPASS_URL, OSM_STORE_NAMES
from ingestion.snowflake_loader import load_dataframe

logger = logging.getLogger(__name__)


def build_overpass_query(store_names: list[str], bbox: str) -> str:
    """
    Build an Overpass QL query to find grocery stores in Belgium.

    Searches for nodes and ways with matching name tags within the Belgian bbox.
    """
    name_filters = "".join(
        f'  node["shop"~"supermarket|convenience"]["name"="{name}"]({bbox});\n'
        f'  way["shop"~"supermarket|convenience"]["name"="{name}"]({bbox});\n'
        for name in store_names
    )

    return f"""
[out:json][timeout:120];
(
{name_filters});
out center tags;
"""


def fetch_stores() -> pd.DataFrame:
    """
    Query Overpass API for Belgian grocery stores.

    Returns DataFrame with columns:
        osm_id, store_name, branch, lat, lng, city, postcode, street, housenumber
    """
    query = build_overpass_query(OSM_STORE_NAMES, OSM_BELGIUM_BBOX)
    logger.info("Querying Overpass API for %d store chains...", len(OSM_STORE_NAMES))

    with httpx.Client(timeout=180) as client:
        resp = client.post(OSM_OVERPASS_URL, data={"data": query})
        resp.raise_for_status()
        data = resp.json()

    elements = data.get("elements", [])
    logger.info("Received %d elements from Overpass", len(elements))

    if not elements:
        return pd.DataFrame()

    records = []
    for el in elements:
        tags = el.get("tags", {})

        # Get coordinates (nodes have lat/lon directly, ways have center)
        if el["type"] == "node":
            lat = el.get("lat")
            lng = el.get("lon")
        else:
            center = el.get("center", {})
            lat = center.get("lat")
            lng = center.get("lon")

        records.append({
            "osm_id": el.get("id"),
            "osm_type": el.get("type"),
            "store_name": tags.get("name", ""),
            "branch": tags.get("branch", ""),
            "brand": tags.get("brand", ""),
            "lat": lat,
            "lng": lng,
            "street": tags.get("addr:street", ""),
            "housenumber": tags.get("addr:housenumber", ""),
            "postcode": tags.get("addr:postcode", ""),
            "city": tags.get("addr:city", ""),
            "province": tags.get("addr:province", ""),
            "phone": tags.get("phone", ""),
            "opening_hours": tags.get("opening_hours", ""),
        })

    df = pd.DataFrame(records)
    logger.info("Parsed %d store locations", len(df))
    return df


def run(overwrite: bool = True):
    """Fetch Belgian store locations from OSM and load into Snowflake RAW."""
    df = fetch_stores()
    if df.empty:
        logger.warning("No OSM store data to load.")
        return

    rows = load_dataframe(df, table_name="osm_stores", overwrite=overwrite)
    logger.info("Loaded %d store locations into RAW.OSM_STORES", rows)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
