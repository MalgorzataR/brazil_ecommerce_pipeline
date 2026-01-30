-- Test: Fact table must have data after transformation
-- Fails if the fact table is empty (critical business check)

{{ config(severity = 'error') }}

with row_count_check as (
    select count(*) as total_rows
    from {{ ref('fct_orders_reviews') }}
)

select *
from row_count_check
where total_rows = 0
