"""
Generate multilingual brand name embeddings and upsert to Pinecone.

Uses the paraphrase-multilingual-MiniLM-L12-v2 model to handle Dutch, French,
and English brand names. Each brand in the canonical brand lookup gets an
embedding vector in the Pinecone index.

Usage:
    python -m master_data.brand_embeddings
"""

import logging

import pandas as pd
from pinecone import Pinecone, ServerlessSpec
from sentence_transformers import SentenceTransformer

from ingestion.config import PINECONE_API_KEY, PINECONE_INDEX_NAME

logger = logging.getLogger(__name__)

MODEL_NAME = "paraphrase-multilingual-MiniLM-L12-v2"
EMBEDDING_DIM = 384
BATCH_SIZE = 100


def get_or_create_index(pc: Pinecone) -> object:
    """Get existing Pinecone index or create it."""
    existing = [idx.name for idx in pc.list_indexes()]

    if PINECONE_INDEX_NAME not in existing:
        logger.info("Creating Pinecone index '%s' (dim=%d)...", PINECONE_INDEX_NAME, EMBEDDING_DIM)
        pc.create_index(
            name=PINECONE_INDEX_NAME,
            dimension=EMBEDDING_DIM,
            metric="cosine",
            spec=ServerlessSpec(cloud="gcp", region="europe-west1"),
        )
    else:
        logger.info("Using existing index '%s'", PINECONE_INDEX_NAME)

    return pc.Index(PINECONE_INDEX_NAME)


def generate_embeddings(brand_names: list[str], model: SentenceTransformer) -> list:
    """Generate embeddings for a list of brand names."""
    logger.info("Generating embeddings for %d brands...", len(brand_names))
    embeddings = model.encode(brand_names, show_progress_bar=True, normalize_embeddings=True)
    return embeddings.tolist()


def upsert_to_pinecone(
    index,
    brand_names: list[str],
    embeddings: list,
    metadata: list[dict] | None = None,
):
    """Upsert brand embeddings to Pinecone in batches."""
    vectors = []
    for i, (name, emb) in enumerate(zip(brand_names, embeddings)):
        meta = metadata[i] if metadata else {}
        meta["brand_name"] = name
        vectors.append({
            "id": f"brand_{i}",
            "values": emb,
            "metadata": meta,
        })

    # Upsert in batches
    for batch_start in range(0, len(vectors), BATCH_SIZE):
        batch = vectors[batch_start : batch_start + BATCH_SIZE]
        index.upsert(vectors=batch)
        logger.info(
            "Upserted batch %d-%d (%d vectors)",
            batch_start, batch_start + len(batch), len(batch),
        )

    logger.info("Total vectors upserted: %d", len(vectors))


def run(brand_lookup_csv: str = "transform/seeds/seed_brand_lookup.csv"):
    """
    Load brand lookup CSV, generate embeddings, and upsert to Pinecone.

    Args:
        brand_lookup_csv: Path to the seed brand lookup CSV.
    """
    # Load brands
    df = pd.read_csv(brand_lookup_csv)
    brand_names = df["brand_name"].tolist()
    logger.info("Loaded %d brands from %s", len(brand_names), brand_lookup_csv)

    # Build metadata from CSV columns
    metadata = []
    for _, row in df.iterrows():
        metadata.append({
            "is_private_label": bool(row.get("is_private_label", False)),
            "retailer_owner": str(row.get("retailer_owner", "")),
            "manufacturer": str(row.get("manufacturer", "")),
        })

    # Generate embeddings
    model = SentenceTransformer(MODEL_NAME)
    embeddings = generate_embeddings(brand_names, model)

    # Upsert to Pinecone
    pc = Pinecone(api_key=PINECONE_API_KEY)
    index = get_or_create_index(pc)
    upsert_to_pinecone(index, brand_names, embeddings, metadata)

    logger.info("Brand embeddings pipeline complete.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
