{{
    config(
        materialized='view'
    )
}}

/*
    Enriched transactions with all dimension attributes joined in.

    Used as the base for mart aggregations. This avoids repeating
    dimension joins in every mart model.
*/

with facts as (

    select * from {{ ref('fact_transactions') }}

),

time_dim as (

    select * from {{ ref('dim_time') }}

),

store_dim as (

    select * from {{ ref('dim_store') }}

),

brand_dim as (

    select * from {{ ref('dim_brand') }}

),

category_dim as (

    select * from {{ ref('dim_category') }}

),

enriched as (

    select
        -- Fact keys & measures
        f.transaction_id,
        f.date_key,
        f.receipt_id,
        f.user_key,
        f.item_price,
        f.quantity,
        f.unit_price,
        f.price_per_unit_measure,
        f.health_score,
        f.is_premium,
        f.is_discount,
        f.panel_weight,

        -- Time attributes
        t.year_month,
        t.year,
        t.month_number,
        t.month_name,
        t.quarter_number,
        t.week_of_year,
        t.is_weekend,
        t.is_belgian_holiday,

        -- Store attributes
        f.store_name,
        s.retailer_group,
        s.store_type,
        s.is_discounter,

        -- Brand attributes
        f.brand_name,
        b.is_private_label,
        b.retailer_owner      as brand_retailer_owner,
        b.manufacturer,

        -- Category attributes
        f.granular_category,
        f.parent_category,
        c.group_name

    from facts f
    left join time_dim t
        on f.date_key = t.date_key
    left join store_dim s
        on f.store_name = s.store_name
    left join brand_dim b
        on f.brand_name = b.brand_name
    left join category_dim c
        on f.granular_category = c.granular_category

)

select * from enriched
