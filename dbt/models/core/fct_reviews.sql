{{
    config(
        materialized='table',
    )
}}

with orders as (
    select *,
    {{ dbt_utils.generate_surrogate_key(['date(order_purchase_timestamp)']) }} as purchase_date_id
    from {{ ref('stg_orders') }}
), 
reviews as (
    select *
    from {{ ref('stg_reviews') }}
),
dates as (
    select *
    from {{ ref('dim_dates') }}
),
orders_with_reviews as (
    select 
        ord.order_id,
        ord.customer_id,
        ord.order_status,
        ord.purchase_date_id,
        ord.order_purchase_date,
        ord.order_purchase_timestamp,
        ord.order_approved_at,
        ord.order_delivered_carrier_date,
        ord.order_delivered_customer_date,
        ord.order_estimated_delivery_date,
        rev.review_id,
        rev.review_score,
        rev.review_score_desc,
        rev.review_creation_date,
        rev.review_answer_timestamp,
        dat.week_end_date,
        dat.month_end_date
    from orders as ord
    left join reviews as rev
    on ord.order_id = rev.order_id
    right join dates as dat
    on dat.date_day = ord.order_purchase_date
    where ord.order_status = 'delivered'
        and dat.month_end_date >= '2017-01-31'
        and dat.month_end_date <= '2018-08-31'
)

select
    *
from orders_with_reviews
