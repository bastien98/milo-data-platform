{{
    config(
        materialized='view'
    )
}}

/*
    Staging model for receipts.

    - Only COMPLETED receipts
    - Parses store_branch for location matching
    - Casts types
*/

with source as (

    select * from {{ source('raw', 'receipts') }}

),

cleaned as (

    select
        id                                  as receipt_id,
        user_id,
        trim(store_name)                    as store_name,
        receipt_date::date                  as receipt_date,
        trim(receipt_time)                  as receipt_time,
        total_amount::float                 as total_amount,
        trim(payment_method)                as payment_method,
        coalesce(total_savings, 0)::float   as total_savings,
        trim(store_branch)                  as store_branch,
        upper(status)                       as status,
        trim(source)                        as source,
        created_at::timestamp_ntz           as created_at

    from source
    where upper(status) = 'COMPLETED'

)

select * from cleaned
