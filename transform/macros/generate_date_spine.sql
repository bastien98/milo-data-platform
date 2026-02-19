{% macro generate_date_spine(start_date, end_date) %}
    /*
        Generate a date spine (one row per day) between start_date and end_date.
        Uses Snowflake's GENERATOR function for efficiency.
    */
    select
        dateadd(
            'day',
            row_number() over (order by seq4()) - 1,
            '{{ start_date }}'::date
        ) as date_day
    from table(generator(rowcount =>
        datediff('day', '{{ start_date }}'::date, '{{ end_date }}'::date) + 1
    ))
{% endmacro %}
