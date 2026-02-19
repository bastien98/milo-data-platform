/*
    Test: no transaction dates should be in the future.

    Future dates indicate either a clock sync issue or bad OCR extraction.
*/

select
    transaction_id,
    date_key,
    store_name
from {{ ref('fact_transactions') }}
where date_key > current_date()
