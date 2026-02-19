{{
    config(
        materialized='table'
    )
}}

/*
    Reference prices: median price per brand × store combination.

    Used as a baseline for future promo detection — if an observed price is
    significantly below the reference price, the item may have been on promotion.

    Computed over rolling 90-day windows to account for gradual price changes.
*/

with recent_prices as (

    select
        brand_name,
        store_name,
        granular_category,
        unit_price,
        price_per_unit_measure,
        date_key
    from {{ ref('fact_transactions') }}
    where brand_name is not null
      and brand_name != ''
      and unit_price > 0
      and date_key >= dateadd('day', -90, current_date())

)

select
    brand_name,
    store_name,
    granular_category,

    -- Price statistics
    median(unit_price)                      as median_unit_price,
    avg(unit_price)                         as avg_unit_price,
    stddev(unit_price)                      as stddev_unit_price,
    min(unit_price)                         as min_unit_price,
    max(unit_price)                         as max_unit_price,
    count(*)                                as observation_count,

    -- Price per unit measure (where available)
    median(price_per_unit_measure)          as median_price_per_unit_measure,
    avg(price_per_unit_measure)             as avg_price_per_unit_measure

from recent_prices
group by brand_name, store_name, granular_category
having count(*) >= 3  -- Minimum observations for a reliable reference
