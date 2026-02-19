{{
    config(
        materialized='table'
    )
}}

/*
    Daily aggregation at broader grain: day × parent_category × store.

    Uses parent_category (31 values) instead of granular (~200) to ensure
    sufficient cell size at daily resolution.
*/

with enriched as (

    select * from {{ ref('int_transactions_enriched') }}

),

final as (

    select
        date_key,
        year_month,
        parent_category,
        group_name,
        store_name,
        retailer_group,
        is_discounter,
        is_weekend,
        is_belgian_holiday,

        -- Metrics
        count(distinct user_key)            as unique_buyers,
        sum(item_price)                     as total_spend,
        sum(quantity)                        as total_units,
        avg(unit_price)                      as avg_unit_price,
        count(*)                             as total_transactions,
        count(distinct receipt_id)           as total_receipts,

        -- Price & quality
        avg(health_score)                    as avg_health_score,
        sum(case when is_premium then item_price else 0 end)
            / nullif(sum(item_price), 0) * 100
                                             as premium_spend_pct

    from enriched
    group by
        date_key,
        year_month,
        parent_category,
        group_name,
        store_name,
        retailer_group,
        is_discounter,
        is_weekend,
        is_belgian_holiday

)

select * from final
