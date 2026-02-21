# Product → Category Master Data

## The Problem

Gemini assigns `granular_category` from 238 options per line item. This is non-deterministic — the same product can receive different categories across receipts. For a data product sold to FMCG clients, category assignment must be 100% deterministic.

## The Solution: `seed_product_category_lookup.csv`

Exact-match lookup table. One row per confirmed product → category mapping.

```
normalized_name,granular_category,confirmed_by
volle melk,Milk Whole,auto
yoghurt natuur,Yoghurt Natural,auto
chips paprika,Chips Potato,auto
crème fraîche,Cream & Crème Fraîche,manual
douchegel,Shower Gel,auto
```

Fields:
- **normalized_name** (PK) — Exact match on Gemini's `normalized_name` output (lowercase, cleaned)
- **granular_category** — The confirmed category from `seed_category_hierarchy.csv`
- **confirmed_by** — `auto` (Gemini was consistent) or `manual` (human resolved a conflict)

### Why normalized_name only, not (normalized_name + master_brand)?

The product type determines the category, not the brand. "Volle melk" is Milk Whole whether it's Boni, Danone, or Colruyt. The brand is irrelevant for categorization.

If an edge case arises where brand matters (see Edge Cases below), the fix is to improve the Gemini prompt to produce more specific normalized_names — not to add brand to the lookup key.

## How It Fits in the Pipeline

```
Receipt line item: "BONI YOGH NAT 500G"
        │
        ▼
  Gemini extracts:
    normalized_name: "yoghurt natuur"
    normalized_brand: "boni"
    granular_category: "Yoghurt Natural"  ← non-deterministic
        │
        ├─── Brand path (Pinecone):
        │    "boni" → Pinecone → master_brand: "Boni" ✓
        │
        └─── Category path (lookup):
             "yoghurt natuur" → seed_product_category_lookup.csv
                  │
                  ├─ FOUND → override Gemini's category with master data ✓ deterministic
                  └─ NOT FOUND → use Gemini's category, track for auto-confirmation
```

Result: both master_brand AND granular_category come from master data for known products.

## How the Dataset Grows

### Phase 1: Initial seeding (first ~50 receipts)

1. Process receipts, let Gemini categorize everything
2. No product lookup exists yet — all categories come from Gemini
3. All (normalized_name, granular_category) pairs stored in transactions

### Phase 2: Auto-confirmation (`category_matcher.py`)

The script runs after ingestion (weekly, or on-demand):

1. Query all transactions, group by `normalized_name`
2. For each normalized_name, count distinct `granular_category` values Gemini has assigned
3. Apply rules:

```
CONSISTENT (same category N+ times, zero conflicts):
  → auto-add to seed_product_category_lookup.csv with confirmed_by = "auto"
  → threshold: N = 3 (seen 3+ times, always same category)

CONFLICT (multiple categories assigned):
  → write to categories_for_review.csv
  → include: normalized_name, each category + count, example receipts
  → human reviews and picks the correct category

NEW (seen < 3 times, no conflicts yet):
  → skip, wait for more data
```

### Phase 3: Steady state

```
New receipt arrives
  → 90%+ of normalized_names already in product lookup → deterministic
  → ~5-8% are new products → Gemini categorizes, tracked for auto-confirmation
  → ~2-3% trigger conflicts → flagged for review
```

### Growth estimate

| Phase | Unique products | Auto-confirmed | Manual review |
|-------|----------------|----------------|---------------|
| Demo (50 receipts) | ~300-500 | ~270-450 | ~30-50 |
| Launch (1,000 receipts) | ~1,500-2,000 | ~1,350-1,800 | ~150-200 |
| Scale (10,000 receipts) | ~3,000-5,000 | ~2,700-4,500 | ~300-500 |

The long tail flattens fast — most new receipts contain products already seen. After ~1,000 receipts, new unique products slow to a trickle.

### categories_for_review.csv (output of category_matcher.py)

```
normalized_name,category_1,count_1,category_2,count_2,example_receipt_id
crème fraîche,Cream & Crème Fraîche,8,Cooking Cream,3,receipt_abc123
salade,Salad & Leafy Greens,5,Meals Salads,4,receipt_def456
```

Human reviews and decides:
- **Pick the correct category** → add to seed_product_category_lookup.csv with confirmed_by = "manual"
- **Ambiguous — Gemini prompt needs fixing** → update Gemini prompt to produce more specific normalized_names (see Edge Cases)

## Edge Cases

### 1. Multilingual product names (Dutch / French)

Belgium has Dutch and French receipts. The same product can appear as different normalized_names:
- Dutch: "volle melk" → Milk Whole
- French: "lait entier" → Milk Whole

These are DIFFERENT normalized_names that map to the SAME category. Both get their own row in the lookup. This is correct — no special handling needed. The product lookup grows in both languages naturally.

**Risk**: Gemini might normalize a French product to a Dutch name (or vice versa). If this happens inconsistently, it causes conflicts. Fix by adding a Gemini prompt rule: "Always use the language of the receipt for normalized_name."

### 2. Same word, different category based on context

```
"crème" on a Dove product    → Face Cream (Personal Care)
"crème" on a Boni product    → Cooking Cream (Dairy)
"salade" from produce aisle  → Salad & Leafy Greens (Fresh Food)
"salade" from deli counter   → Meals Salads (Charcuterie & Deli)
```

The lookup key is normalized_name only. If the same normalized_name maps to different categories, it's a conflict.

**Fix**: Don't add brand to the lookup key. Instead, fix the Gemini prompt to produce more specific normalized_names:
- "crème" → "gezichtscrème" (face cream) vs "kookroom" (cooking cream)
- "salade" → "sla" (lettuce) vs "bereide salade" (prepared salad)

This keeps the lookup simple (one key) and pushes the disambiguation upstream to where it belongs (extraction).

### 3. Temperature/state variants of same product

```
"pizza margherita" from chilled section → Ready Meals Chilled
"pizza margherita" from frozen section  → Ready Meals Frozen
```

The receipt doesn't tell you the aisle. Gemini might guess differently each time.

**Fix**: If the distinction matters for your data product, add a Gemini prompt rule: "If the product could be chilled or frozen, default to [chilled/frozen] unless the receipt explicitly states otherwise." Pick one default and be consistent. If the distinction doesn't matter, merge the categories.

### 4. Compound / multi-category products

```
"kaas-ham croissant" → Bakery? Charcuterie? Cheese? Sandwiches?
"granola met chocolade" → Breakfast & Cereal? Chocolate & Sweets?
```

**Fix**: Define a hierarchy of precedence in the Gemini prompt:
- If it's a prepared/composite item → categorize by its PRIMARY component or consumption occasion
- "kaas-ham croissant" = Sandwiches (it's a filled bakery item, eaten as a meal)
- "granola met chocolade" = Breakfast & Cereal (primary consumption is breakfast)

These rules go in the Gemini prompt, not in master data. Once Gemini is consistent, auto-confirmation handles the rest.

### 5. Gemini producing inconsistent normalized_names

The prompt says "same product = same normalized_name" but LLMs aren't perfectly deterministic:
- "volle melk" vs "hele melk" vs "volle melk 1l"
- "kipfilet" vs "kip filet" vs "kippenfilet"

**Detection**: `category_matcher.py` won't catch this directly (different names = different lookup entries). But you'll notice it when similar-looking normalized_names appear as separate entries in the product lookup.

**Fix**:
- Tighten the Gemini prompt with explicit normalization rules (no quantities in name, specific word choices)
- Periodic audit: sort seed_product_category_lookup.csv alphabetically, scan for near-duplicates
- Future enhancement: fuzzy deduplication script that flags similar normalized_names for merging

### 6. Deposit and discount lines

```
"leeggoed" (bottle deposit)  → Deposits
"hoeveelheidsvoordeel"       → Promos & Discounts
```

These are already handled: `is_deposit=true` and `is_discount=true` flags from Gemini. They're excluded from the data product (filtered in fact_transactions). Don't add them to the product lookup — they're not real products.

### 7. Unbranded / generic items

```
"bananen" → no brand (normalized_brand = null)
"wortelen" → no brand
"brood wit" → no brand
```

These still get a category mapping in the product lookup. The brand being null doesn't affect the product → category relationship. "Bananen" is always "Fruit Bananas" regardless.

### 8. Store-specific product names

Different stores print different names for the same product:
- Colruyt: "HALFV MELK 1L"
- Delhaize: "LAIT DEMI-ECR 1L"
- Lidl: "HV MELK 1L"

Gemini should normalize all of these to the same `normalized_name` (e.g., "halfvolle melk"). If it doesn't, you get separate entries in the product lookup — each mapping to the same category. This is technically redundant but not harmful.

**Fix**: If Gemini consistently fails to normalize specific store formats, improve the prompt with examples from that store.

## No Pinecone Needed

Unlike brand matching, category assignment does NOT need vector search:
- Only 238 categories — not a large search space
- The mapping is exact (normalized_name → category), not fuzzy
- You WANT strict matching — a near-match means Gemini produced an inconsistent name, which should be caught, not silently accepted
- Simple CSV lookup is faster, cheaper, and more transparent than vector search

## No Aliases Needed

Unlike brand names (which get abbreviated on receipts: "DAN." for Danone), normalized_names are already cleaned by Gemini. If Gemini produces inconsistent normalized_names, the fix is in the prompt, not in an alias table. Adding aliases would mask a Gemini extraction problem that should be fixed at the source.

## Relationship to Other Seed Files

```
seed_product_category_lookup.csv         seed_category_hierarchy.csv
┌──────────────────────────────┐         ┌──────────────────────────┐
│ normalized_name (PK)         │         │ granular_category (PK)   │
│ granular_category (FK) ──────┼────────→│ parent_category          │
│ confirmed_by                 │         │ group_name               │
└──────────────────────────────┘         └──────────────────────────┘

This file validates WHAT category a specific product gets.
seed_brand_category_hierarchy_lookup.csv validates WHICH categories a brand operates in.
Both reference the same seed_category_hierarchy.csv for the canonical category list.
```

## Summary

| Aspect | Brand matching | Category assignment |
|--------|---------------|-------------------|
| Matching method | Pinecone vector search | Exact CSV lookup |
| Key | normalized_brand (fuzzy) | normalized_name (exact) |
| Growth | brand_matcher.py + manual review | category_matcher.py + auto-confirm |
| Aliases needed? | Yes (receipt abbreviations) | No (Gemini normalizes) |
| Ignore list needed? | Yes (OCR junk) | No (handled by deposit/discount flags) |
| Scale | ~100s of brands | ~1,000s of products |
| Manual effort | Review every new brand | Review only conflicts (~5-10%) |
