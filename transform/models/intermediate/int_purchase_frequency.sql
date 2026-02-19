{{
    config(
        materialized='table'
    )
}}

/*
    Purchase frequency: shopping trips per buyer per brand × store × month.

    A "trip" is a distinct receipt. This measures how often buyers come back
    to buy a brand at a specific store.
*/

with trips as (

    select
        year_month,
        granular_category,
        store_name,
        brand_name,
        user_key,
        count(distinct receipt_id) as trips
    from {{ ref('int_transactions_enriched') }}
    where brand_name is not null
      and brand_name != ''
    group by year_month, granular_category, store_name, brand_name, user_key

)

select
    year_month,
    granular_category,
    store_name,
    brand_name,
    count(distinct user_key)        as unique_buyers,
    sum(trips)                      as total_trips,
    avg(trips)                      as avg_trips_per_buyer
from trips
group by year_month, granular_category, store_name, brand_name
