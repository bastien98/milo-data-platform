{{
    config(
        materialized='table'
    )
}}

/*
    Category dimension from seed_category_hierarchy.csv.

    Three-level hierarchy:
    - group_name (8): Fresh Food, Pantry & Staples, Frozen, Drinks, Snacks, ...
    - parent_category (31): Fruits, Vegetables, Meat & Poultry, ...
    - granular_category (~200): Fruit Apples Pears, Tomatoes, Beef, ...

    Analytics-excluded categories (Promos & Discounts, Deposits) are filtered out.
    Grain: one row per granular_category.
*/

with seed as (

    select
        granular_category,
        parent_category,
        group_name
    from {{ ref('seed_category_hierarchy') }}

),

-- Exclude analytics-excluded categories
filtered as (

    select *
    from seed
    where parent_category not in (
        'Promos & Discounts',
        'Deposits (Statiegeld/Vidange)'
    )
    and granular_category not in (
        'Discount',
        'Coupon',
        'Loyalty Discount',
        'Promotional Offer',
        'Multi-Buy Deal',
        'Bottle Deposit',
        'Can Deposit',
        'Crate Deposit',
        'Deposit Refund'
    )

)

select * from filtered
