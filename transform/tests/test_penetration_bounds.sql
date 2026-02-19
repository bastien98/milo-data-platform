/*
    Test: penetration_pct must be between 0 and 100 (inclusive).

    A penetration above 100% means more unique buyers than total panelists,
    which indicates a bug in the panel_size calculation.
*/

select
    year_month,
    granular_category,
    store_name,
    brand_name,
    penetration_pct
from {{ ref('mart_category_performance') }}
where penetration_pct < 0
   or penetration_pct > 100
