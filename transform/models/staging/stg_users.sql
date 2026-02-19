{{
    config(
        materialized='view'
    )
}}

/*
    Staging model for users.

    Joins users + user_profiles via the firebase_uid FK.
    Note: user_profiles.user_id references users.firebase_uid (NOT users.id).
*/

with users as (

    select * from {{ source('raw', 'users') }}

),

profiles as (

    select * from {{ source('raw', 'user_profiles') }}

),

joined as (

    select
        u.id                                as user_id,
        u.firebase_uid,
        u.email,
        trim(upper(p.gender))              as gender,
        u.created_at::timestamp_ntz        as user_created_at,
        p.created_at::timestamp_ntz        as profile_created_at

    from users u
    left join profiles p
        on u.firebase_uid = p.user_id

)

select * from joined
