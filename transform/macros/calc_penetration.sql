{% macro calc_penetration(unique_buyers, panel_size) %}
    /*
        Calculate penetration rate as a percentage.
        penetration = unique_buyers / panel_size × 100

        Returns NULL if panel_size is 0 (avoids division by zero).
    */
    round(
        {{ unique_buyers }} / nullif({{ panel_size }}, 0) * 100,
        2
    )
{% endmacro %}
