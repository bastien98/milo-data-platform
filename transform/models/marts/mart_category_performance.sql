{{
    config(
        materialized='table'
    )
}}

/*
    THE CORE DATA PRODUCT: Category Performance Tracker.

    Grain: month × granular_category × store_name × brand_name

    14 metrics:
    1.  unique_buyers          — distinct panelists who bought this cell
    2.  panel_size             — total active panelists that month
    3.  penetration_pct        — unique_buyers / panel_size × 100
    4.  purchase_frequency     — avg shopping trips per buyer
    5.  avg_spend_per_buyer    — total_spend / unique_buyers
    6.  total_spend            — sum of item_price
    7.  total_units            — sum of quantity
    8.  avg_unit_price         — avg of unit_price
    9.  avg_price_per_unit_measure — avg price per kg/L
    10. category_share_pct     — cell_spend / category_total_spend × 100
    11. premium_spend_pct      — premium item spend / total_spend × 100
    12. discount_pct           — discounted item spend / total_spend × 100
    13. avg_health_score       — avg health score of items
    14. total_transactions     — count of transaction rows

    Plus dimension attributes for client-friendly pivoting.
*/

with enriched as (

    select * from {{ ref('int_transactions_enriched') }}

),

panel as (

    select * from {{ ref('int_panel_size') }}

),

-- Core aggregation at the target grain
cell_metrics as (

    select
        year_month,
        granular_category,
        parent_category,
        group_name,
        store_name,
        retailer_group,
        store_type,
        is_discounter,
        brand_name,
        is_private_label,
        brand_retailer_owner,
        manufacturer,

        -- Metric 1: unique buyers
        count(distinct user_key)                            as unique_buyers,

        -- Metric 6: total spend
        sum(item_price)                                     as total_spend,

        -- Metric 7: total units
        sum(quantity)                                        as total_units,

        -- Metric 8: avg unit price
        avg(unit_price)                                      as avg_unit_price,

        -- Metric 9: avg price per unit measure
        avg(price_per_unit_measure)                          as avg_price_per_unit_measure,

        -- Metric 11: premium spend
        sum(case when is_premium then item_price else 0 end) as premium_spend,

        -- Metric 12: discount spend
        sum(case when is_discount then item_price else 0 end) as discount_spend,

        -- Metric 13: avg health score
        avg(health_score)                                    as avg_health_score,

        -- Metric 14: total transaction rows
        count(*)                                             as total_transactions,

        -- For purchase frequency: count distinct receipts
        count(distinct receipt_id)                           as total_trips

    from enriched
    where brand_name is not null
      and brand_name != ''
    group by
        year_month,
        granular_category,
        parent_category,
        group_name,
        store_name,
        retailer_group,
        store_type,
        is_discounter,
        brand_name,
        is_private_label,
        brand_retailer_owner,
        manufacturer

),

-- Category total spend per month × granular_category (for share calculation)
category_totals as (

    select
        year_month,
        granular_category,
        sum(item_price) as category_total_spend
    from enriched
    group by year_month, granular_category

),

final as (

    select
        -- Dimensions
        c.year_month,
        c.granular_category,
        c.parent_category,
        c.group_name,
        c.store_name,
        c.retailer_group,
        c.store_type,
        c.is_discounter,
        c.brand_name,
        c.is_private_label,
        c.brand_retailer_owner,
        c.manufacturer,

        -- Metric 1: unique buyers
        c.unique_buyers,

        -- Metric 2: panel size
        p.panel_size,

        -- Metric 3: penetration
        round(c.unique_buyers / nullif(p.panel_size, 0) * 100, 2)
            as penetration_pct,

        -- Metric 4: purchase frequency (trips per buyer)
        round(c.total_trips / nullif(c.unique_buyers, 0), 2)
            as purchase_frequency,

        -- Metric 5: avg spend per buyer
        round(c.total_spend / nullif(c.unique_buyers, 0), 2)
            as avg_spend_per_buyer,

        -- Metric 6-9: spend, units, prices
        round(c.total_spend, 2)                 as total_spend,
        c.total_units,
        round(c.avg_unit_price, 2)              as avg_unit_price,
        round(c.avg_price_per_unit_measure, 2)  as avg_price_per_unit_measure,

        -- Metric 10: category share
        round(c.total_spend / nullif(ct.category_total_spend, 0) * 100, 2)
            as category_share_pct,

        -- Metric 11: premium spend %
        round(c.premium_spend / nullif(c.total_spend, 0) * 100, 2)
            as premium_spend_pct,

        -- Metric 12: discount %
        round(c.discount_spend / nullif(c.total_spend, 0) * 100, 2)
            as discount_pct,

        -- Metric 13: avg health score
        round(c.avg_health_score, 1)            as avg_health_score,

        -- Metric 14: total transactions
        c.total_transactions

    from cell_metrics c
    left join panel p
        on c.year_month = p.year_month
    left join category_totals ct
        on c.year_month = ct.year_month
        and c.granular_category = ct.granular_category

)

select * from final
