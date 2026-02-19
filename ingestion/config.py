"""
Connection configurations for all data sources.

All secrets are loaded from environment variables (.env file).
"""

import os

from dotenv import load_dotenv

load_dotenv()


# ---------------------------------------------------------------------------
# Railway PostgreSQL (transactional database)
# ---------------------------------------------------------------------------

RAILWAY_DB_URL = os.environ["RAILWAY_DATABASE_URL"]


# ---------------------------------------------------------------------------
# Snowflake (data warehouse)
# ---------------------------------------------------------------------------

SNOWFLAKE_CONFIG = {
    "account": os.environ["SNOWFLAKE_ACCOUNT"],
    "user": os.environ["SNOWFLAKE_USER"],
    "password": os.environ["SNOWFLAKE_PASSWORD"],
    "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
    "database": os.environ.get("SNOWFLAKE_DATABASE", "SCANDALICIOUS_DW"),
    "role": os.environ.get("SNOWFLAKE_ROLE", "TRANSFORM"),
}

SNOWFLAKE_RAW_SCHEMA = "RAW"


# ---------------------------------------------------------------------------
# Pinecone (vector DB for brand matching)
# ---------------------------------------------------------------------------

PINECONE_API_KEY = os.environ.get("PINECONE_API_KEY", "")
PINECONE_INDEX_NAME = os.environ.get("PINECONE_INDEX_NAME", "brand-embeddings")
PINECONE_ENVIRONMENT = os.environ.get("PINECONE_ENVIRONMENT", "gcp-starter")


# ---------------------------------------------------------------------------
# Open Food Facts
# ---------------------------------------------------------------------------

OFF_API_BASE = "https://world.openfoodfacts.org/api/v2"
OFF_COUNTRY = "belgium"
OFF_PAGE_SIZE = int(os.environ.get("OFF_PAGE_SIZE", "100"))
OFF_MAX_PAGES = int(os.environ.get("OFF_MAX_PAGES", "50"))


# ---------------------------------------------------------------------------
# OpenStreetMap Overpass API
# ---------------------------------------------------------------------------

OSM_OVERPASS_URL = os.environ.get(
    "OSM_OVERPASS_URL", "https://overpass-api.de/api/interpreter"
)

# Belgian bounding box (south, west, north, east)
OSM_BELGIUM_BBOX = "49.5,2.5,51.5,6.4"

# Grocery store chains to query
OSM_STORE_NAMES = [
    "Colruyt",
    "Delhaize",
    "AD Delhaize",
    "Proxy Delhaize",
    "Albert Heijn",
    "Carrefour",
    "Carrefour Market",
    "Carrefour Express",
    "Lidl",
    "Aldi",
    "Intermarché",
    "Match",
    "Spar",
    "OKay",
    "Bio-Planet",
    "Cru",
]
