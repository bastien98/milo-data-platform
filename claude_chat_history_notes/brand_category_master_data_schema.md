# Brand & Category Master Data Schema

## Five Seed Files

### 1. `seed_brand_master.csv` — WHO is this brand?
One row per brand (~103 currently). Source of truth for brand identity.

```
master_brand,manufacturer,retailer_owner,is_private_label,embedding_string
Activia,Danone,,FALSE,activia
Danone,Danone,,FALSE,danone
Jupiler,AB InBev,,FALSE,jupiler
Boni,,Colruyt Group,TRUE,boni
```

Fields:
- **master_brand** — The consumer-facing brand name (what appears on packaging/receipt). Extract the most SPECIFIC brand: "Activia" not "Danone", "Leffe" not "AB InBev". Sub-brands are treated as their own brand (Nielsen/IRI pattern).
- **manufacturer** — Corporate parent that makes the product. NULL for private labels. Enables "total Danone portfolio share" queries via GROUP BY manufacturer.
- **retailer_owner** — Which retail chain owns this brand. NULL for manufacturer brands. Only set for private labels (Boni → Colruyt Group).
- **is_private_label** — Derivable from `retailer_owner IS NOT NULL` but kept for query ergonomics. Clients filter on this constantly.
- **embedding_string** — Lowercase canonical brand name that gets vectorized in Pinecone. Always = lowercase master_brand. One embedding per brand. Additional aliases (abbreviations, OCR variants) go in `seed_brand_aliases.csv`.

Key rules:
- manufacturer and retailer_owner are MUTUALLY EXCLUSIVE
- Sub-brands (Activia, Leffe, Dove Men+Care) are separate master_brand entries
- The manufacturer field provides the roll-up across a company's brand portfolio

### 2. `seed_brand_aliases.csv` — WHAT OTHER NAMES does this brand go by?
Alias table. One row per alternative brand name. Handles abbreviations, OCR variants, and spelling differences.

```
master_brand,alias_string
Vandemoortele,vdm
Coca-Cola,cocacola
Coca-Cola,coca cola
Côte d'Or,cote dor
```

- master_brand FK → seed_brand_master.csv
- alias_string is the alternate text that gets embedded in Pinecone (lowercase)
- Composite PK: (master_brand, alias_string)
- `brand_embeddings.py` reads BOTH seed_brand_master + seed_brand_aliases and creates vectors for all of them, all pointing to the same master_brand metadata

When to add an alias:
- Receipt OCR consistently produces a variant Gemini can't normalize (e.g., "VDM" for Vandemoortele)
- A brand name appears without spaces or accents on receipts (e.g., "cocacola", "cote dor")
- brand_matcher.py flags a match with score < 0.95 that a human confirms is the same brand

### 3. `seed_brand_ignore.csv` — WHAT brand strings should we skip?
Blocklist. One row per brand string that has been reviewed and is not a real brand. Prevents junk, generic items, and OCR noise from showing up in `brands_for_review.csv` repeatedly.

```
ignored_brand
nutriboot korst
unknown
n/a
gratis artikel
```

- PK: ignored_brand
- Case-insensitive matching (lowercased before comparison)
- `brand_matcher.py` loads this list and filters out matching brands BEFORE querying Pinecone
- Not loaded into Pinecone — these strings are simply skipped

When to add to the ignore list:
- A `brands_for_review.csv` entry is clearly not a brand (generic product description, OCR garbage, store promo text)
- You've reviewed it and confirmed it has no value for brand analytics

### 4. `seed_brand_category_hierarchy_lookup.csv` — WHAT does this brand sell?
Bridge table. One row per brand × granular_category combination. Two fields only.

```
master_brand,granular_category
Danone,Yoghurt Natural
Activia,Yoghurt Fruit
Activia,Yoghurt Drinks
Boni,Whole Milk
Boni,Yoghurt Natural
```

- Composite natural key: (master_brand, granular_category)
- No surrogate key in the CSV — generated in the dbt dimension model in Snowflake
- master_brand FK → seed_brand_master.csv
- granular_category FK → seed_category_hierarchy.csv

### 5. `seed_category_hierarchy.csv` — HOW do categories roll up?
Fixed product taxonomy. ~238 granular categories. Rarely changes.

```
granular_category,parent_category,group_name
Yoghurt Natural,Dairy Eggs & Cheese,Fresh Food
Beer Pils,Alcohol,Drinks
Shower Gel,Pharmacy & Hygiene,Personal Care
```

Three levels: group_name (8) → parent_category (31) → granular_category (238)

## Relationships

```
seed_brand_master.csv                seed_category_hierarchy.csv
┌─────────────────────┐              ┌──────────────────────────┐
│ master_brand (PK)   │              │ granular_category (PK)   │
│ manufacturer        │              │ parent_category          │
│ retailer_owner      │              │ group_name               │
│ is_private_label    │              └────────────▲─────────────┘
│ embedding_string    │                           │
└──────────▲──────────┘                           │ FK
           │                                      │
           │ FK   seed_brand_aliases.csv           │
           ├──────┌───────────────────────────┐   │
           │      │ master_brand (FK)         │   │
           │      │ alias_string              │   │
           │      └───────────────────────────┘   │
           │        (composite PK: both cols)     │
           │                                      │
           │ FK   seed_brand_category_hierarchy_lookup.csv
           │      ┌─────────────────────────────┐ │
           └──────┤ master_brand (FK)           │ │
                  │ granular_category (FK) ──────┼─┘
                  └─────────────────────────────┘
                    (composite PK: both columns)
```

## Why Brand × Category is the Product Grain

Receipt OCR (Gemini) cannot reliably extract SKU-level data. A line item like "BONI YOGH NAT 500G" gives you brand + category, not an exact product. The 500g is a fact-level measure (weight_or_volume), not a dimension identifier. Brand × category IS the best resolution achievable from receipt data.

## How the Dataset Grows

### Receipt processing flow
1. Receipt image → Gemini extracts `normalized_brand` + `granular_category` per line item
2. Prompt instructs: extract the MOST SPECIFIC brand ("activia" not "danone")
3. Category is reliable (Gemini picks from 238 fixed options). Brand is fuzzy.

### Brand matching via Pinecone
1. `brand_embeddings.py` reads seed_brand_master.csv + seed_brand_aliases.csv → embeds all strings → upserts to Pinecone
2. Canonical brands get vectors from `embedding_string`; aliases get vectors from `alias_string` — all share the same master_brand metadata
3. Pinecone stores: vector + metadata (master_brand, manufacturer, retailer_owner, is_private_label)
4. Model: paraphrase-multilingual-MiniLM-L12-v2 (384-dim, handles Dutch/French)
5. Pinecone is brand-level only — NOT brand×category (category already reliable from Gemini)

### When new brands appear
1. `brand_matcher.py` finds unmatched normalized_brand values from transactions
2. Filters out brands in `seed_brand_ignore.csv` (already reviewed, not real brands)
3. Embeds remaining unmatched brands, queries Pinecone (top-k=3)
4. Score ≥ 0.95 → confident match to existing canonical brand (auto-resolved)
5. Score < 0.95 → flagged in brands_for_review.csv for manual curation
6. Human reviews and decides one of three outcomes:
   - **Known brand, variant spelling** → add alias to seed_brand_aliases.csv
   - **Genuinely new brand** → add to seed_brand_master.csv + seed_brand_category_hierarchy_lookup.csv
   - **Not a real brand** → add to seed_brand_ignore.csv (never flagged again)
7. Changes merged via PR (version controlled)
8. `dbt seed` loads updated CSVs into Snowflake → Pinecone index rebuilt

### Embedding scheme
- Embed just the lowercase brand name. Nothing else.
- Symmetry: Gemini outputs a brand name → Pinecone stores brand name embeddings
- Adding manufacturer/category to the embedding causes cross-brand contamination
- Metadata rides along on the Pinecone vector for free (not embedded, just stored)
- Reranker reorders top-K using metadata + lexical precision
- Aliases stored in seed_brand_aliases.csv → each alias gets its own vector in Pinecone pointing to the same master_brand

### Pinecone vs Snowflake
- Pinecone = brand matching tool (stateless, rebuild anytime from seed CSV)
- Snowflake = serving layer (dim_brand_category used by facts/marts)
- NOT 1:1 copies. Pinecone is brand-level. Snowflake dimension is brand×category level.

## Multilingual Edge Cases (Belgium: Dutch / French / German)

Brand names are proper nouns — they don't change with language. The master data is inherently language-resilient. But there are edge cases to watch for:

- **Private labels with different names per region** — Delhaize uses "365" in both languages, but some retailers have different private label names for Wallonia vs Flanders. If that happens, add aliases in `seed_brand_aliases.csv`.
- **Gemini failing on French abbreviations** — If Gemini sees `YAO NAT BIO` on a French receipt and can't extract the brand, that's a Gemini prompt issue, not a master data issue. Fix it by improving the extraction prompt.
- **Accented brand names** — `Côte d'Or` vs `cote dor` vs `COTE D'OR`. Handled by `seed_brand_aliases.csv` — add the unaccented/variant form as an alias.
- **German receipts (east Belgium)** — Tiny edge case but the multilingual embedding model (`paraphrase-multilingual-MiniLM-L12-v2`) covers it. No master data changes needed.

## Key Design Decisions

| Decision | Reasoning |
|----------|-----------|
| Brand×category as product grain | Receipt OCR doesn't give SKU-level data |
| Sub-brands are their own brands | Avoids nullable sub_brand field (NULL 80% of the time, useless for slicing) |
| manufacturer / retailer_owner as separate fields | They answer different questions: "company market share" vs "private label threat" |
| parent_category NOT in brand master or bridge | Derivable from dim_category via JOIN — one source of truth |
| No surrogate keys in CSVs | Generated in dbt models, not in flat files |
| embedding_string on brand master | Documents what's in Pinecone for the canonical brand name |
| Separate seed_brand_aliases.csv | Keeps brand master 1-row-per-brand (PK intact), aliases are additive vectors in Pinecone |
| Confidence threshold at 0.95 | High bar for auto-matching — only near-exact matches. Safer: more goes to human review |
| Separate seed_brand_ignore.csv | Prevents junk/non-brand strings from cluttering review every time matcher runs. Reviewed once, ignored forever |
| Replaces old seed_brand_lookup.csv | Same 103 brands, better structure (split into brand master + bridge + aliases + ignore) |
