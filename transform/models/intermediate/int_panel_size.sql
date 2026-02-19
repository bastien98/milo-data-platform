{{
    config(
        materialized='table'
    )
}}

/*
    Active panelists per month.

    A panelist is "active" in a month if they submitted at least 1 receipt.
    This is used as the denominator for penetration calculations.
*/

with monthly_activity as (

    select
        year_month,
        user_key
    from {{ ref('int_transactions_enriched') }}
    group by year_month, user_key

)

select
    year_month,
    count(distinct user_key) as panel_size
from monthly_activity
group by year_month
