{{
    config(
        materialized='view'
    )
}}

/*
    Staging model for Open Food Facts brand data.

    Deduplicates to one row per distinct primary brand.
    Includes nutriscore and nova group where available.
*/

with source as (

    select * from {{ source('raw', 'off_products') }}

),

deduplicated as (

    select
        trim(primary_brand)                 as brand_name,
        count(*)                            as product_count,
        mode(nutriscore)                    as typical_nutriscore,
        mode(nova_group)                    as typical_nova_group,
        mode(off_category)                  as typical_category

    from source
    where primary_brand is not null
      and trim(primary_brand) != ''
    group by trim(primary_brand)

)

select * from deduplicated
