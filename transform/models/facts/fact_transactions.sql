{{
    config(
        materialized='table'
    )
}}

/*
    Core fact table: one row per item purchased.

    Joins stg_transactions to all 5 dimensions:
    - dim_time (via transaction_date)
    - dim_store (via store_name)
    - dim_brand (via normalized_brand)
    - dim_category (via granular_category)
    - dim_user (via user_id → user_key)

    Contains all measures needed for the data product.
*/

with transactions as (

    select * from {{ ref('stg_transactions') }}

),

users as (

    select user_id, user_key, panel_weight
    from {{ ref('dim_user') }}

),

final as (

    select
        -- Keys
        t.transaction_id,
        t.transaction_date                      as date_key,
        t.store_name,
        t.normalized_brand                      as brand_name,
        t.granular_category,
        t.parent_category,
        u.user_key,
        t.receipt_id,

        -- Measures
        t.item_price,
        t.quantity,
        t.unit_price,
        t.price_per_unit_measure,
        t.health_score,
        t.weight_or_volume,
        t.unit_of_measure,

        -- Flags
        t.is_premium,
        t.is_discount,

        -- Item details (for debugging / future products)
        t.item_name,
        t.normalized_name,

        -- Panel weight for weighted aggregations
        u.panel_weight,

        -- Metadata
        t.created_at

    from transactions t
    inner join users u
        on t.user_id = u.user_id

)

select * from final
