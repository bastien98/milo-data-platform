/*
    Test: every granular_category in fact_transactions must map to a valid
    entry in dim_category.

    Orphan granular categories mean the seed_category_hierarchy.csv is
    out of sync with what Gemini is extracting.
*/

select
    f.granular_category,
    count(*) as orphan_transactions
from {{ ref('fact_transactions') }} f
left join {{ ref('dim_category') }} c
    on f.granular_category = c.granular_category
where c.granular_category is null
group by f.granular_category
