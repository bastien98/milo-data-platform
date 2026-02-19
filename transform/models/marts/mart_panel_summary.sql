{{
    config(
        materialized='table'
    )
}}

/*
    Monthly panel health dashboard.

    One row per month. Tracks:
    - Active panelists, receipt volume, item volume
    - Store coverage (how many distinct stores)
    - Category coverage (how many distinct granular categories)
    - Brand coverage
    - Data quality indicators
*/

with enriched as (

    select * from {{ ref('int_transactions_enriched') }}

),

monthly as (

    select
        year_month,

        -- Panel size
        count(distinct user_key)                    as active_panelists,

        -- Volume
        count(distinct receipt_id)                  as total_receipts,
        count(*)                                    as total_items,
        sum(item_price)                             as total_spend,

        -- Coverage
        count(distinct store_name)                  as distinct_stores,
        count(distinct retailer_group)              as distinct_retailer_groups,
        count(distinct granular_category)            as distinct_granular_categories,
        count(distinct parent_category)              as distinct_parent_categories,
        count(distinct brand_name)                   as distinct_brands,

        -- Averages per panelist
        round(count(distinct receipt_id)::float
            / nullif(count(distinct user_key), 0), 1)
                                                     as avg_receipts_per_panelist,
        round(count(*)::float
            / nullif(count(distinct user_key), 0), 1)
                                                     as avg_items_per_panelist,
        round(sum(item_price)
            / nullif(count(distinct user_key), 0), 2)
                                                     as avg_spend_per_panelist,

        -- Data quality
        round(sum(case when brand_name is not null and brand_name != ''
            then 1 else 0 end)::float / nullif(count(*), 0) * 100, 1)
                                                     as brand_fill_rate_pct,
        round(sum(case when health_score is not null
            then 1 else 0 end)::float / nullif(count(*), 0) * 100, 1)
                                                     as health_score_fill_rate_pct,
        round(sum(case when price_per_unit_measure is not null
            then 1 else 0 end)::float / nullif(count(*), 0) * 100, 1)
                                                     as price_per_unit_fill_rate_pct

    from enriched
    group by year_month

)

select * from monthly
order by year_month
