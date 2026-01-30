{{
    config(
        materialized="view"
    )
}}
with orders as (
  select *,
    row_number() over(
        partition by 
            order_id,
            customer_id,
            order_status,
            order_purchase_timestamp,
            order_approved_at,
            order_delivered_carrier_date,
            order_delivered_customer_date,
            order_estimated_delivery_date
        ) as rn
  from {{ source("staging", "orders") }}
)

select
    {{ dbt.safe_cast("order_id", api.Column.translate_type("string")) }} as order_id,
    {{ dbt.safe_cast("customer_id", api.Column.translate_type("string")) }} as customer_id,
    {{ dbt.safe_cast("order_status", api.Column.translate_type("string")) }} as order_status,
    {{ dbt.safe_cast("order_purchase_timestamp", api.Column.translate_type("date")) }} as order_purchase_date,
    {{ dbt.safe_cast("order_purchase_timestamp", api.Column.translate_type("timestamp")) }} as order_purchase_timestamp,
    {{ dbt.safe_cast("order_approved_at", api.Column.translate_type("timestamp")) }} as order_approved_at,
    {{ dbt.safe_cast("order_delivered_carrier_date", api.Column.translate_type("timestamp")) }} as order_delivered_carrier_date,
    {{ dbt.safe_cast("order_delivered_customer_date", api.Column.translate_type("timestamp")) }} as order_delivered_customer_date,
    {{ dbt.safe_cast("order_estimated_delivery_date", api.Column.translate_type("timestamp")) }} as order_estimated_delivery_date
from orders
where rn = 1






