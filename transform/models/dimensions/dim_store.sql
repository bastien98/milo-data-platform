{{
    config(
        materialized='table'
    )
}}

/*
    Store dimension.

    Combines:
    - seed_store_lookup (manual): retailer_group, store_type, is_discounter
    - stg_stores_osm (API): lat/lng, city, province

    For stores not in OSM, location fields are null.
    Grain: one row per store_name.
*/

with store_seed as (

    select
        store_name,
        retailer_group,
        store_type,
        is_discounter
    from {{ ref('seed_store_lookup') }}

),

osm_agg as (

    -- Aggregate OSM data to store_name level (pick most common city/province)
    select
        store_name,
        count(*)                    as location_count,
        mode(city)                  as typical_city,
        mode(province)              as typical_province,
        avg(latitude)               as avg_latitude,
        avg(longitude)              as avg_longitude
    from {{ ref('stg_stores_osm') }}
    group by store_name

),

final as (

    select
        s.store_name,
        s.retailer_group,
        s.store_type,
        s.is_discounter,
        o.location_count,
        o.typical_city,
        o.typical_province,
        o.avg_latitude,
        o.avg_longitude

    from store_seed s
    left join osm_agg o
        on upper(s.store_name) = upper(o.store_name)

)

select * from final
