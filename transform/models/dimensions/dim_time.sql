{{
    config(
        materialized='table'
    )
}}

/*
    Date dimension: date spine from earliest transaction to today.

    Includes:
    - Calendar attributes (week, month, quarter, year)
    - Weekend flag
    - Belgian public holiday flag (from seed)
*/

with date_spine as (

    {{ dbt_date.get_date_dimension(
        start_date="2024-01-01",
        end_date="2030-12-31"
    ) }}

),

holidays as (

    select
        holiday_date::date as holiday_date,
        holiday_name,
        holiday_name_nl,
        holiday_name_fr
    from {{ ref('seed_belgian_holidays') }}

),

final as (

    select
        d.date_day                                  as date_key,
        d.day_of_week,
        d.day_of_week_name,
        d.day_of_month,
        d.day_of_year,
        d.week_of_year,
        d.iso_week_of_year,
        d.month_of_year                             as month_number,
        d.month_name,
        d.quarter_of_year                           as quarter_number,
        d.year_number                               as year,

        -- Derived fields
        date_trunc('week', d.date_day)::date        as week_start,
        date_trunc('month', d.date_day)::date       as month_start,
        last_day(d.date_day, 'month')               as month_end,
        date_trunc('quarter', d.date_day)::date     as quarter_start,

        -- Year-month key for aggregations
        to_char(d.date_day, 'YYYY-MM')              as year_month,

        -- Flags
        case when d.day_of_week in (6, 7)
            then true else false
        end                                         as is_weekend,

        case when h.holiday_date is not null
            then true else false
        end                                         as is_belgian_holiday,

        h.holiday_name,
        h.holiday_name_nl,
        h.holiday_name_fr

    from date_spine d
    left join holidays h
        on d.date_day = h.holiday_date

)

select * from final
