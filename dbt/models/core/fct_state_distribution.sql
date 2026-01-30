with fact_orders_reviews as(
    select * 
    from {{ ref("fct_reviews") }}
),
customers as(
    select * 
    from {{ ref("stg_customers") }}
),
brazil_states as(
    select * 
    from {{ ref("brazil_states") }}
),
delivery_metrics AS (
    select
        fct.order_id,
        fct.month_end_date,
        fct.order_purchase_date,
        fct.order_delivered_customer_date,
        cust.customer_id,
        'Brazil' as country,
        bs.state,
        bs.iso_code,
        {{ datediff("fct.order_purchase_timestamp", "fct.order_delivered_customer_date",  "day") }} as delivery_length_days
    from fact_orders_reviews fct
    inner join customers cust
    on fct.customer_id = cust.customer_id
    inner join brazil_states bs
    on bs.iso_code = cust.customer_state
)

select 
    month_end_date,
    state,
    iso_code,
    count(order_id) as number_of_orders,
    approx_quantiles(delivery_length_days, 2)[OFFSET(1)] as median_delivery_days -- index 1 = median
from delivery_metrics
group by 1, 2, 3
