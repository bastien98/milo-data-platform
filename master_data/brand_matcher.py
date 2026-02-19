"""
Match normalized_brand values from transactions to canonical brands via Pinecone.

For each new/unmatched brand extracted by Gemini, we:
1. Generate an embedding for the brand name
2. Query Pinecone for the nearest canonical brand
3. If similarity >= CONFIDENCE_THRESHOLD, accept the match
4. If below threshold, flag for manual review

Usage:
    python -m master_data.brand_matcher
"""

import logging

import pandas as pd
from pinecone import Pinecone
from sentence_transformers import SentenceTransformer

from ingestion.config import PINECONE_API_KEY, PINECONE_INDEX_NAME
from ingestion.snowflake_loader import execute_query

logger = logging.getLogger(__name__)

MODEL_NAME = "paraphrase-multilingual-MiniLM-L12-v2"
CONFIDENCE_THRESHOLD = 0.85
TOP_K = 3


def get_unmatched_brands() -> list[str]:
    """
    Get distinct normalized_brand values from transactions that are not yet
    in the brand lookup seed.
    """
    query = """
        SELECT DISTINCT t.NORMALIZED_BRAND
        FROM RAW.TRANSACTIONS t
        WHERE t.NORMALIZED_BRAND IS NOT NULL
          AND t.NORMALIZED_BRAND != ''
          AND t.NORMALIZED_BRAND NOT IN (
              SELECT BRAND_NAME FROM SCANDALICIOUS_DW.DIMENSIONS.DIM_BRAND
          )
        ORDER BY t.NORMALIZED_BRAND
    """
    df = execute_query(query)
    brands = df["NORMALIZED_BRAND"].tolist()
    logger.info("Found %d unmatched brands", len(brands))
    return brands


def match_brands(
    unmatched: list[str],
    model: SentenceTransformer,
    index,
) -> pd.DataFrame:
    """
    Match each unmatched brand against Pinecone index.

    Returns DataFrame with columns:
        input_brand, matched_brand, similarity, is_confident
    """
    if not unmatched:
        return pd.DataFrame(columns=["input_brand", "matched_brand", "similarity", "is_confident"])

    logger.info("Generating embeddings for %d unmatched brands...", len(unmatched))
    embeddings = model.encode(unmatched, normalize_embeddings=True)

    results = []
    for brand_name, embedding in zip(unmatched, embeddings):
        response = index.query(
            vector=embedding.tolist(),
            top_k=TOP_K,
            include_metadata=True,
        )

        if response.matches:
            best = response.matches[0]
            results.append({
                "input_brand": brand_name,
                "matched_brand": best.metadata.get("brand_name", ""),
                "similarity": round(best.score, 4),
                "is_confident": best.score >= CONFIDENCE_THRESHOLD,
                "is_private_label": best.metadata.get("is_private_label", False),
                "retailer_owner": best.metadata.get("retailer_owner", ""),
                "manufacturer": best.metadata.get("manufacturer", ""),
            })
        else:
            results.append({
                "input_brand": brand_name,
                "matched_brand": "",
                "similarity": 0.0,
                "is_confident": False,
                "is_private_label": False,
                "retailer_owner": "",
                "manufacturer": "",
            })

    df = pd.DataFrame(results)
    confident = df["is_confident"].sum()
    logger.info(
        "Matched %d/%d brands above %.0f%% confidence",
        confident, len(df), CONFIDENCE_THRESHOLD * 100,
    )
    return df


def run():
    """Run the brand matching pipeline."""
    # Get unmatched brands from Snowflake
    unmatched = get_unmatched_brands()
    if not unmatched:
        logger.info("No unmatched brands found. Done.")
        return

    # Load model and index
    model = SentenceTransformer(MODEL_NAME)
    pc = Pinecone(api_key=PINECONE_API_KEY)
    index = pc.Index(PINECONE_INDEX_NAME)

    # Match brands
    matches_df = match_brands(unmatched, model, index)

    # Split into confident matches and review candidates
    confident = matches_df[matches_df["is_confident"]]
    review = matches_df[~matches_df["is_confident"]]

    logger.info(
        "Results: %d confident matches, %d need review",
        len(confident), len(review),
    )

    # Save review candidates for manual inspection
    if not review.empty:
        review.to_csv("exports/output/brands_for_review.csv", index=False)
        logger.info("Saved %d brands for review to exports/output/brands_for_review.csv", len(review))

    # TODO: Upsert confident matches into seed_brand_lookup or Snowflake directly
    if not confident.empty:
        logger.info("Confident matches preview:")
        for _, row in confident.head(10).iterrows():
            logger.info(
                "  %s → %s (%.1f%%)",
                row["input_brand"], row["matched_brand"], row["similarity"] * 100,
            )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
