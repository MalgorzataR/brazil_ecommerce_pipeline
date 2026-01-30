-- Test: Every review must have a corresponding order
-- Fails if any reviews reference non-existent orders

select
    r.review_id,
    r.order_id
from {{ ref('stg_reviews') }} r
left join {{ ref('stg_orders') }} o
    on r.order_id = o.order_id
where o.order_id is null
