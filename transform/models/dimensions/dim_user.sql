{{
    config(
        materialized='table'
    )
}}

/*
    User dimension with anonymized demographics.

    - Replaces user_id with a surrogate key (MD5 hash)
    - Strips PII (first_name, last_name, email)
    - Keeps: gender, signup date, panel_weight
    - panel_weight = 1.0 by default (future: compute from demographics vs Belgian census)

    Grain: one row per user.
*/

with users as (

    select * from {{ ref('stg_users') }}

),

-- Calculate user activity metrics for panel quality
user_activity as (

    select
        user_id,
        count(distinct receipt_id)      as total_receipts,
        count(*)                        as total_items,
        min(transaction_date)           as first_transaction_date,
        max(transaction_date)           as last_transaction_date,
        count(distinct store_name)      as distinct_stores
    from {{ ref('stg_transactions') }}
    group by user_id

),

final as (

    select
        md5(u.user_id::varchar)             as user_key,
        u.user_id,
        u.gender,
        u.user_created_at                   as signup_date,

        -- Activity metrics
        coalesce(a.total_receipts, 0)       as total_receipts,
        coalesce(a.total_items, 0)          as total_items,
        a.first_transaction_date,
        a.last_transaction_date,
        coalesce(a.distinct_stores, 0)      as distinct_stores,

        -- Panel weighting (1.0 = no adjustment, future: demographic weighting)
        1.0                                 as panel_weight,

        -- Is this user active? (at least 1 receipt in last 60 days)
        case
            when a.last_transaction_date >= dateadd('day', -60, current_date())
            then true else false
        end                                 as is_active

    from users u
    left join user_activity a
        on u.user_id = a.user_id

)

select * from final
