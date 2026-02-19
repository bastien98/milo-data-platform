{{
    config(
        materialized='table'
    )
}}

/*
    Brand dimension.

    Combines:
    - seed_brand_lookup (manual): is_private_label, retailer_owner, manufacturer
    - stg_brands_off (API): nutriscore, nova_group from Open Food Facts

    New brands from transactions not in the seed get is_private_label = false.
    Grain: one row per brand_name.
*/

with brand_seed as (

    select
        brand_name,
        is_private_label,
        retailer_owner,
        manufacturer
    from {{ ref('seed_brand_lookup') }}

),

off_brands as (

    select
        brand_name,
        product_count       as off_product_count,
        typical_nutriscore,
        typical_nova_group,
        typical_category    as off_category
    from {{ ref('stg_brands_off') }}

),

-- Get any brands from transactions not in the seed
transaction_brands as (

    select distinct normalized_brand as brand_name
    from {{ ref('stg_transactions') }}
    where normalized_brand is not null
      and normalized_brand != ''

),

-- Union seed brands with any new transaction brands
all_brands as (

    select brand_name from brand_seed
    union
    select brand_name from transaction_brands

),

final as (

    select
        ab.brand_name,
        coalesce(bs.is_private_label, false)    as is_private_label,
        coalesce(bs.retailer_owner, '')         as retailer_owner,
        coalesce(bs.manufacturer, '')           as manufacturer,
        ob.off_product_count,
        ob.typical_nutriscore,
        ob.typical_nova_group,
        ob.off_category

    from all_brands ab
    left join brand_seed bs
        on ab.brand_name = bs.brand_name
    left join off_brands ob
        on lower(ab.brand_name) = lower(ob.brand_name)

)

select * from final
