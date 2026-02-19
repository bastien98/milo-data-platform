# Milo Data Platform

Data engineering platform that transforms Scandalicious receipt data into a sellable consumer panel data product for Belgian FMCG brands.

## Architecture Overview

```
SOURCES                          SNOWFLAKE                           DATA PRODUCTS
═══════                          ═════════                           ═════════════

Railway PostgreSQL ──┐           RAW (1:1 copy of sources)
  transactions       │    ┌──→   ├── transactions
  receipts           ├────┤      ├── receipts
  users              │    │      ├── users
  user_profiles      │    │      ├── user_profiles
                     ┘    │      │
                          │      STAGING (cleaned by dbt)
Open Food Facts API ──────┤      ├── stg_transactions
                          │      ├── stg_receipts
OpenStreetMap API ────────┤      ├── stg_users
                          │      ├── stg_brands_off
Pinecone (vector DB) ─────┘      ├── stg_stores_osm
                                 │
                                 INTERMEDIATE (business logic)
                                 ├── int_transactions_enriched
                                 ├── int_panel_size
                                 ├── int_purchase_frequency
                                 ├── int_reference_prices
                                 │
                                 DIMENSIONS
                                 ├── dim_time ─────────────┐
                                 ├── dim_store ────────────┤
                                 ├── dim_brand ────────────┤
                                 ├── dim_category ─────────┤
                                 ├── dim_user ─────────────┤
                                 │                         │
                                 FACTS                     │
                                 ├── fact_transactions ◄───┘
                                 │
                                 MARTS (data products)       ──→  CSV / PDF / API
                                 ├── mart_category_performance    (clients)
                                 ├── mart_daily_category_store
                                 └── mart_panel_summary
```

## How Each Dimension Is Built

### dim_time — Calendar Dimension

**Source**: Generated date spine (no external data needed)

A calendar table with one row per date. Pre-computed attributes let clients group by week, month, quarter without date math.

| Column | Example | Source |
|--------|---------|--------|
| date_key | 2026-01-14 | Generated |
| day_of_week | Wednesday | Derived |
| week_number | 3 | Derived |
| month | 2026-01 | Derived |
| quarter | Q1 2026 | Derived |
| year | 2026 | Derived |
| is_weekend | false | Derived (Sat/Sun) |
| is_belgian_holiday | false | seed_belgian_holidays.csv |

**Built by**: `transform/models/dimensions/dim_time.sql` using the `generate_date_spine` macro + join to `seed_belgian_holidays`.

---

### dim_store — Store Dimension

**Sources**: Manual seed CSV + OpenStreetMap API

**Step 1: Manual seed** (`seed_store_lookup.csv`)
Manually maintained ~15 rows covering every store chain in our data:

```csv
store_name,retailer_group,store_type,is_discounter
colruyt,Colruyt Group,supermarket,true
okay,Colruyt Group,proximity,true
bio-planet,Colruyt Group,organic,false
delhaize,Ahold Delhaize,supermarket,false
albert heijn,Ahold Delhaize,supermarket,false
aldi,Aldi,hard_discount,true
lidl,Schwarz Group,hard_discount,true
carrefour,Carrefour Group,hypermarket,false
spar,Spar Group,proximity,false
intermarche,Les Mousquetaires,supermarket,false
cora,Louis Delhaize Group,hypermarket,false
match,Louis Delhaize Group,supermarket,false
jumbo,Jumbo,supermarket,false
```

**Step 2: OSM enrichment** (`master_data/store_enricher.py`)
For each unique `store_branch` from receipts (e.g., "Colruyt Leuven"), query OpenStreetMap Overpass API:

```python
# Overpass query: find all Colruyt stores in Belgium
query = """
[out:json];
area["name"="België / Belgique / Belgien"]["admin_level"="2"]->.belgium;
node["shop"="supermarket"]["name"~"Colruyt",i](area.belgium);
out body;
"""
```

Returns: latitude, longitude, address, city. We then derive `province` from coordinates.

**Step 3: dbt joins them** (`dim_store.sql`)
```sql
select
    s.store_name,
    s.retailer_group,
    s.store_type,
    s.is_discounter,
    osm.latitude,
    osm.longitude,
    osm.address,
    osm.city,
    osm.province
from {{ ref('seed_store_lookup') }} s
left join {{ ref('stg_stores_osm') }} osm
    on s.store_name = osm.store_chain
```

---

### dim_brand — Brand Dimension (with Vector DB)

**Sources**: Manual seed CSV + Open Food Facts API + Pinecone vector matching

This is the most complex dimension because brand names on receipts are messy and need canonicalization.

**Step 1: Seed top brands manually** (`seed_brand_lookup.csv`)

```csv
brand_name,is_private_label,retailer_owner,manufacturer
danone,false,,Danone SA
alpro,false,,Danone SA
jupiler,false,,AB InBev
cara,false,,AB InBev
leffe,false,,AB InBev
coca-cola,false,,Coca-Cola European Partners
boni,true,Colruyt,
everyday,true,Colruyt,
365,true,Delhaize,
delhaize,true,Delhaize,
carrefour,true,Carrefour,
```

Start with ~100 brands. Covers ~70-80% of transactions.

**Step 2: Enrich from Open Food Facts** (`ingestion/open_food_facts.py`)
Download Belgian products from the Open Food Facts API:

```python
# Fetch Belgian products with brand info
url = "https://world.openfoodfacts.org/cgi/search.pl"
params = {
    "tagtype_0": "countries",
    "tag_contains_0": "contains",
    "tag_0": "belgium",
    "fields": "product_name,brands,categories,quantity",
    "page_size": 100,
    "json": 1
}
```

Extracts: brand names, product categories, quantities. Used to fill gaps in the manual seed.

**Step 3: Build embeddings for brand matching** (`master_data/brand_embeddings.py`)

```python
from sentence_transformers import SentenceTransformer

# Multilingual model (Dutch + French Belgian receipt text)
model = SentenceTransformer('paraphrase-multilingual-MiniLM-L12-v2')

# Embed all canonical brand names
brands = ["danone", "alpro", "jupiler", "boni", ...]
embeddings = model.encode(brands)

# Upsert to Pinecone
index.upsert(vectors=[
    {"id": brand, "values": embedding.tolist(), "metadata": {"brand": brand}}
    for brand, embedding in zip(brands, embeddings)
])
```

**Step 4: Match new brands via Pinecone** (`master_data/brand_matcher.py`)

When a new `normalized_brand` appears in transactions that doesn't match any seed entry:

```python
def match_brand(unknown_brand: str) -> str:
    """Match an unknown brand to the nearest canonical brand in Pinecone."""
    embedding = model.encode(unknown_brand)
    results = index.query(vector=embedding.tolist(), top_k=3)

    best_match = results.matches[0]
    if best_match.score > 0.85:  # high confidence threshold
        return best_match.metadata["brand"]  # canonical name
    else:
        return unknown_brand  # new brand, add to seed for manual review
```

**Use cases for vector matching:**
- Receipt says `"dan."` → matches to `"danone"` (score: 0.91)
- Receipt says `"coca cola zero"` → matches to `"coca-cola"` (score: 0.88)
- Receipt says `"boni sel."` → matches to `"boni"` (score: 0.93)
- Receipt says `"artisanaal"` → no good match (score: 0.42) → flagged for manual review

**Step 5: dbt builds the final dimension** (`dim_brand.sql`)
```sql
select
    coalesce(seed.brand_name, txn_brands.brand_name) as brand_name,
    coalesce(seed.is_private_label, false) as is_private_label,
    seed.retailer_owner,
    seed.manufacturer,
    off.product_count  -- how many OFF products match this brand
from {{ ref('seed_brand_lookup') }} seed
full outer join (
    select distinct normalized_brand as brand_name
    from {{ ref('stg_transactions') }}
    where normalized_brand is not null
) txn_brands on seed.brand_name = txn_brands.brand_name
left join {{ ref('stg_brands_off') }} off
    on seed.brand_name = off.brand_name
```

---

### dim_category — Category Dimension

**Source**: Generated from the app's `categories.py` hierarchy

The app defines a 3-level hierarchy: 8 Groups → 31 Parent Categories → ~200 Granular Categories. This is exported to `seed_category_hierarchy.csv`:

```csv
granular_name,parent_category,group_name
Fruit Apples Pears,Fruits,Fresh Food
Fruit Citrus,Fruits,Fresh Food
Yoghurt Natural,"Dairy, Eggs & Cheese",Fresh Food
Yoghurt Fruit,"Dairy, Eggs & Cheese",Fresh Food
Beer Pils,"Alcohol (Beer, Cider, Wine, Whisky, Vodka, Gin, Cava, Champagne)",Drinks
Beer Abbey Trappist,"Alcohol (Beer, Cider, Wine, Whisky, Vodka, Gin, Cava, Champagne)",Drinks
Chips,"Chips, Nuts & Aperitif",Snacks
...
```

**Built by**: `dim_category.sql` simply selects from the seed with a surrogate key:
```sql
select
    {{ dbt_utils.generate_surrogate_key(['granular_name']) }} as category_key,
    granular_name,
    parent_category,
    group_name
from {{ ref('seed_category_hierarchy') }}
```

**Maintenance**: When a new granular category is added to `categories.py` in the backend, regenerate the CSV by running `master_data/seed_categories.py`.

---

### dim_user — User/Panelist Dimension

**Source**: Railway PostgreSQL users + user_profiles tables

| Column | Source | Notes |
|--------|--------|-------|
| user_key | Generated surrogate | Anonymized — never expose real user_id |
| gender | user_profiles.gender | MALE / FEMALE / PREFER_NOT_TO_SAY |
| age_range | *Future: app survey* | Not yet collected |
| household_size | *Future: app survey* | Not yet collected |
| postcode | *Future: app survey* | Not yet collected |
| province | Derived from postcode | Not yet available |
| panel_weight | Computed | Default 1.0, future: weight by demographics vs census |

**Important**: User data is anonymized in the data product. The `user_key` is a surrogate key (hash of user_id), never the actual Firebase UID. Names are never included in the dimension.

**Panel weighting** (future): Once demographics are collected, compare panel composition to Belgian census data and compute weights:
```
If 25-34 males are 40% of panel but 18% of Belgium:
  panel_weight = 18/40 = 0.45 (weight them down)
If 55-64 females are 5% of panel but 15% of Belgium:
  panel_weight = 15/5 = 3.0 (weight them up)
```

---

## How the Fact Table Is Built

`fact_transactions.sql` is the core of the warehouse. One row = one item purchased on a receipt.

```sql
select
    -- Surrogate keys (link to dimensions)
    {{ dbt_utils.generate_surrogate_key(['t.id']) }} as transaction_key,
    du.user_key,
    dt.date_key,
    ds.store_key,
    db.brand_key,
    dc.category_key,

    -- Degenerate dimensions (no separate table needed)
    t.receipt_id,
    t.original_description,
    t.normalized_name,

    -- Measures
    t.item_price,
    t.quantity,
    t.unit_price,
    t.is_premium,
    t.is_discount,
    t.is_deposit,
    t.health_score,
    t.unit_of_measure,
    t.weight_or_volume,
    t.price_per_unit_measure

from {{ ref('stg_transactions') }} t
-- Join to dimensions via business keys
left join {{ ref('dim_time') }} dt on t.date = dt.date_key
left join {{ ref('dim_store') }} ds on t.store_name = ds.store_name
left join {{ ref('dim_brand') }} db on t.normalized_brand = db.brand_name
left join {{ ref('dim_category') }} dc on t.granular_category = dc.granular_name
left join {{ ref('dim_user') }} du on t.user_id = du.source_user_id
-- Exclude non-product lines
where not t.is_deposit
  and t.category not in ('Promos & Discounts', 'Deposits (Statiegeld/Vidange)')
```

---

## How the Final Data Product Is Built

### mart_category_performance (THE CORE DATA PRODUCT)

**Grain**: One row per month × granular_category × store × brand

This single table IS the data product. Clients open it in Excel and pivot/filter to answer their questions.

```sql
with panel as (
    select month, panel_size from {{ ref('int_panel_size') }}
),
freq as (
    select * from {{ ref('int_purchase_frequency') }}
),
core as (
    select
        date_trunc('month', ft.date_key)::date as month,
        dc.group_name,
        dc.parent_category,
        dc.granular_name as granular_category,
        ds.store_name,
        ds.retailer_group,
        ds.is_discounter,
        db.brand_name as brand,
        db.is_private_label,

        count(distinct du.user_key) as unique_buyers,
        round(sum(ft.item_price), 2) as total_spend,
        sum(ft.quantity) as total_units,
        round(sum(ft.item_price) / nullif(sum(ft.quantity), 0), 2) as avg_unit_price,
        round(sum(ft.item_price) / nullif(count(distinct du.user_key), 0), 2) as avg_spend_per_buyer,
        round(avg(ft.price_per_unit_measure), 2) as avg_price_per_unit_measure,
        round(sum(case when ft.is_premium then ft.item_price else 0 end)
              / nullif(sum(ft.item_price), 0) * 100, 1) as premium_spend_pct,
        round(sum(case when ft.is_discount then 1 else 0 end)::numeric
              / nullif(count(*), 0) * 100, 1) as discount_pct,
        round(avg(ft.health_score), 1) as avg_health_score

    from {{ ref('fact_transactions') }} ft
    join {{ ref('dim_time') }} dt on ft.date_key = dt.date_key
    join {{ ref('dim_store') }} ds on ft.store_key = ds.store_key
    join {{ ref('dim_brand') }} db on ft.brand_key = db.brand_key
    join {{ ref('dim_category') }} dc on ft.category_key = dc.category_key
    join {{ ref('dim_user') }} du on ft.user_key = du.user_key
    group by 1,2,3,4,5,6,7,8,9
),
cat_totals as (
    select month, parent_category, store_name,
           sum(total_spend) as category_total_spend
    from core group by 1,2,3
)
select
    c.*,
    p.panel_size,
    round(c.unique_buyers::numeric / p.panel_size * 100, 1) as penetration_pct,
    f.avg_purchase_frequency,
    round(c.total_spend / nullif(ct.category_total_spend, 0) * 100, 1) as category_share_pct
from core c
join panel p on c.month = p.month
join cat_totals ct on c.month = ct.month
    and c.parent_category = ct.parent_category
    and c.store_name = ct.store_name
left join freq f on c.month = f.month
    and c.granular_category = f.granular_category
    and c.store_name = f.store_name
    and c.brand = f.brand
```

### Metrics in the final data product

| Metric | Aggregation | Business question |
|--------|-------------|-------------------|
| unique_buyers | COUNT(DISTINCT user_key) | How many people buy this? |
| panel_size | Total active panelists that month | Denominator for penetration |
| penetration_pct | unique_buyers / panel_size * 100 | What % of Belgians buy this? |
| purchase_frequency | AVG distinct receipts per buyer | How often do buyers come back? |
| avg_spend_per_buyer | total_spend / unique_buyers | How much does each buyer spend? |
| total_spend | SUM(item_price) | How big is this in euros? |
| total_units | SUM(quantity) | How many units sold? |
| avg_unit_price | total_spend / total_units | Average price per unit |
| avg_price_per_unit_measure | AVG(price_per_unit_measure) | Price per kg or liter |
| category_share_pct | Cell spend / category total | Brand's share of category |
| premium_spend_pct | Premium spend / total * 100 | Is this premiumizing? |
| discount_pct | Discount items / total * 100 | How promo-dependent? |
| avg_health_score | AVG(health_score) | How healthy is this? |

---

## Running the Pipeline

### Prerequisites
```bash
# Python environment
cd milo-data-platform
python -m venv .venv
source .venv/bin/activate
pip install -e .

# Copy env template and fill in secrets
cp .env.example .env
# Edit .env with Railway DB URL, Snowflake creds, Pinecone API key

# dbt setup
cd transform
dbt deps          # install dbt packages
dbt debug         # verify Snowflake connection
```

### Step-by-step execution

```bash
# 1. Extract from sources → Snowflake RAW
python -m ingestion.railway_extract        # transactions, receipts, users, profiles
python -m ingestion.open_food_facts        # Belgian product catalog
python -m ingestion.openstreetmap          # Belgian store locations

# 2. Build/refresh master data
python -m master_data.seed_brands          # generate seed_brand_lookup.csv
python -m master_data.seed_stores          # generate seed_store_lookup.csv
python -m master_data.brand_embeddings     # upsert brand embeddings to Pinecone
python -m master_data.brand_matcher        # match new brands to canonical entries

# 3. Run dbt transformations
cd transform
dbt seed                                   # load CSV seeds into Snowflake
dbt run                                    # build staging → intermediate → dims → facts → marts
dbt test                                   # validate data quality

# 4. Export data products
python -m exports.csv_exporter             # export mart tables to CSV
python -m exports.pdf_report               # generate branded PDF reports
```

---

## Scheduling

### Recommended: GitHub Actions (simplest for solo developer)

Three cron-triggered workflows:

| Workflow | Schedule | What it does |
|----------|----------|--------------|
| `daily_ingestion.yml` | Every day 6:00 UTC | Extract Railway → Snowflake RAW |
| `weekly_master_data.yml` | Every Monday 7:00 UTC | Refresh brand/store master data, run brand matcher |
| `monthly_dbt_run.yml` | 1st of month 8:00 UTC | dbt seed + run + test, export data products |

### Alternative: Dagster (if you need more control)

Dagster provides a local web UI, dependency tracking, and retry logic. Use this if pipelines grow complex or you need manual triggering with monitoring.

---

## Adding New Dimensions or Sources

### Adding a new dimension (e.g., dim_product for SKU-level)
1. Create seed CSV: `transform/seeds/seed_product_lookup.csv`
2. Create ingestion script if needed: `ingestion/new_source.py`
3. Create staging model: `transform/models/staging/stg_new_source.sql`
4. Create dimension model: `transform/models/dimensions/dim_product.sql`
5. Update `fact_transactions.sql` to join the new dimension
6. Update mart models to include new dimension attributes
7. Add tests in `_dimensions.yml`

### Adding a new data source (e.g., Daltix price data)
1. Create ingestion script: `ingestion/daltix.py`
2. Add RAW table in Snowflake
3. Create staging model: `transform/models/staging/stg_daltix_prices.sql`
4. Use in intermediate/mart models as needed

### Adding a new data product (e.g., promo intelligence)
1. Create mart model: `transform/models/marts/mart_promo_intelligence.sql`
2. Add tests in `_marts.yml`
3. Add export template: `exports/templates/promo_report.html`
4. Update orchestration to include new model

---

## Feasible Plan to Build a Demo Data Product for Investors

### The Problem

You need to show investors a working data product, but you don't have 5,000 panelists yet. With only a handful of real users, the data lacks statistical significance. Here's how to build a convincing demo anyway.

### Strategy: Blend Real Data + Synthetic Data, Be Transparent

The goal is NOT to fake having 5,000 users. The goal is to show:
1. The pipeline works end-to-end (receipt → datamart)
2. The output format is what clients want
3. The business model math works

### Phase 1: Collect 50-100 Real Receipts (1-2 weeks)

**You + friends + family.** Ask 10-15 people to scan 5-10 receipts each over 2 weeks. Mix of stores:
- 3-4 people shop at Colruyt
- 3-4 people shop at Delhaize
- 2-3 people shop at Aldi or Lidl
- 2-3 people shop at Carrefour

This gives you 50-100 real receipts = ~400-800 real transaction rows. Not enough for statistical significance, but enough to:
- Prove the AI extraction works (Gemini correctly identifies brands, categories, prices)
- Show real Belgian product names, real prices, real stores
- Demonstrate the pipeline from receipt → structured data → datamart

### Phase 2: Generate Synthetic Panel Data (1 weekend)

Use your existing `testbench/generate_test_user_csv.py` as a starting point, but make it realistic:

```python
# Generate 5,000 synthetic panelists with realistic Belgian shopping patterns
# Based on known Belgian market data:
# - Colruyt 26% market share, Delhaize 23%, Carrefour 18%, Aldi 11%, Lidl 9%
# - Average household shops 12x/month
# - Average basket: 8-12 items, €45-65
# - Private label share: 39.8% (Belgian average)
# Use REAL product names and prices from your 50-100 real receipts
```

The synthetic data should:
- Use **real product names and prices** extracted from your real receipts (not made-up products)
- Follow **known Belgian market share distributions** (cite sources: GfK, PLMA, Comeos)
- Include realistic **demographic distribution** (age, household size by Belgian census)
- Cover **6 months** of history to show trends

### Phase 3: Run the Full Pipeline (1 day)

1. Load real + synthetic data into Snowflake RAW
2. Run dbt to build staging → dimensions → facts → marts
3. Export the `mart_category_performance` table to Excel
4. Create 2-3 sample PDF reports for specific categories

### Phase 4: Build the Demo Deck (2-3 days)

**Slide 1: The Problem**
> 3,979 Belgian food companies. 99% have zero consumer panel data. YouGov costs €100K+.

**Slide 2: The Product**
> Show the actual datamart output in Excel. Let the investor pivot and filter it.

**Slide 3: Real Data Proof**
> "Here are 100 real receipts from 15 Belgian households, processed through our AI pipeline."
> Show: original receipt photo → extracted data → aggregated metrics
> This proves the technology works.

**Slide 4: Scaled Simulation**
> "Here's what the data product looks like at 5,000 panelists."
> Show: the synthetic-based datamart with realistic Belgian market dynamics
> Caveat clearly: "This is simulated using known market distributions. Real panel data will show actual consumer behavior."

**Slide 5: Sample Client Report**
> Show a branded PDF report for a fictional Belgian cheese brand:
> - Their penetration across retailers
> - Their brand share vs competitors
> - Price positioning
> - Private label threat

**Slide 6: Validation**
> "We showed this report format to X Belgian food producers. Y said they would pay for it."
> Include quotes from your validation interviews.

### What To Be Transparent About

Tell investors:
- "The 100 real receipts prove the technology pipeline works end-to-end"
- "The scaled simulation shows the output format and business model"
- "We need €50-75K to fund rewards and reach 5,000 real panelists"
- "At 15 paying clients × €400/month, the business is self-sustaining"

Do NOT:
- Claim the synthetic data is real
- Show statistical conclusions from 100 receipts (too small)
- Pretend you have scale you don't have

### The 100-Receipt Demo Is Surprisingly Powerful

Even with 100 receipts, you can demonstrate:

| What you show | Why it's convincing |
|---------------|-------------------|
| Receipt photo → structured JSON | "Our AI correctly extracts brand, price, category from messy Belgian receipts" |
| Same product at different stores | "We see Danone yoghurt at €2.49 at Colruyt and €2.69 at Delhaize" |
| Brand detection accuracy | "We correctly identified 47 unique brands across 100 receipts" |
| Category taxonomy | "Every item classified into our 200-category Belgian grocery taxonomy" |
| The datamart format | "This is the exact Excel file a client would receive monthly" |
| Pipeline working | "Receipt scanned at 2pm, data available in Snowflake by 6pm" |

An investor doesn't need to see 5,000 users to believe the product works. They need to see:
1. The technology works (100 real receipts prove this)
2. The output is valuable (sample report proves this)
3. Clients want it (5 validation interviews prove this)
4. The math works (cost model proves this)

### Timeline

```
Week 1-2:  Collect 50-100 real receipts from friends/family
Week 2:    Generate synthetic panel data with realistic distributions
Week 3:    Run full pipeline, build demo datamart + sample reports
Week 3-4:  Talk to 5-10 Belgian food producers, collect validation quotes
Week 4:    Build investor demo deck
```

Total cost: €0 (just your time). No Snowflake costs needed for demo — use the free trial.
