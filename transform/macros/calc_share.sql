{% macro calc_share(cell_value, total_value) %}
    /*
        Calculate share/percentage of total.
        share = cell_value / total_value × 100

        Returns NULL if total_value is 0 (avoids division by zero).
    */
    round(
        {{ cell_value }} / nullif({{ total_value }}, 0) * 100,
        2
    )
{% endmacro %}
