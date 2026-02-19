{{
    config(
        materialized='view'
    )
}}

/*
    Staging model for OpenStreetMap store locations.

    Cleans and standardizes store location data from OSM Overpass.
*/

with source as (

    select * from {{ source('raw', 'osm_stores') }}

),

cleaned as (

    select
        osm_id,
        osm_type,
        trim(store_name)                    as store_name,
        trim(branch)                        as branch,
        trim(brand)                         as brand,
        lat::float                          as latitude,
        lng::float                          as longitude,
        trim(street)                        as street,
        trim(housenumber)                   as housenumber,
        trim(postcode)                      as postcode,
        trim(city)                          as city,
        trim(province)                      as province,
        trim(opening_hours)                 as opening_hours

    from source
    where lat is not null
      and lng is not null
      and store_name is not null

)

select * from cleaned
