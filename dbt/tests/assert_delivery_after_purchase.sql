-- Test: Delivery date must be after purchase date
-- Fails if any orders were delivered before they were purchased

select
    order_id,
    order_purchase_timestamp,
    order_delivered_customer_date
from {{ ref('stg_orders') }}
where order_delivered_customer_date is not null
    and order_delivered_customer_date < order_purchase_timestamp
