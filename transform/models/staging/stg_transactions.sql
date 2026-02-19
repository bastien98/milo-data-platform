{{
    config(
        materialized='view'
    )
}}

/*
    Staging model for transactions.

    - Filters out deposits and promo discount lines (excluded categories)
    - Only includes items from COMPLETED receipts
    - Handles legacy category migration:
        ALCOHOL → "Alcohol (Beer, Cider, Wine, Whisky, Vodka, Gin, Cava, Champagne)"
        MEAT_FISH → "Meat & Poultry (Raw)"
    - Casts types and handles nulls
*/

with source as (

    select * from {{ source('raw', 'transactions') }}

),

completed_receipts as (

    select id as receipt_id
    from {{ source('raw', 'receipts') }}
    where upper(status) = 'COMPLETED'

),

cleaned as (

    select
        s.id                                as transaction_id,
        s.user_id,
        s.receipt_id,
        trim(s.store_name)                  as store_name,
        trim(s.item_name)                   as item_name,
        s.item_price::float                 as item_price,
        coalesce(s.quantity, 1)::int        as quantity,
        s.unit_price::float                 as unit_price,
        trim(s.normalized_name)             as normalized_name,
        trim(s.normalized_brand)            as normalized_brand,
        coalesce(s.is_premium, false)       as is_premium,
        coalesce(s.is_discount, false)      as is_discount,
        coalesce(s.is_deposit, false)       as is_deposit,

        -- Handle legacy category migration
        case
            when s.category = 'ALCOHOL'
                then 'Alcohol (Beer, Cider, Wine, Whisky, Vodka, Gin, Cava, Champagne)'
            when s.category = 'MEAT_FISH'
                then 'Meat & Poultry (Raw)'
            else trim(s.category)
        end                                 as parent_category,

        trim(s.granular_category)           as granular_category,
        s.health_score::float               as health_score,
        trim(s.unit_of_measure)             as unit_of_measure,
        s.weight_or_volume::float           as weight_or_volume,
        s.price_per_unit_measure::float     as price_per_unit_measure,
        s.date::date                        as transaction_date,
        s.created_at::timestamp_ntz         as created_at

    from source s
    inner join completed_receipts r
        on s.receipt_id = r.receipt_id

)

select *
from cleaned
where
    -- Exclude deposits and promo discount lines from analytics
    is_deposit = false
    and parent_category not in (
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
