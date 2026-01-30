with fact_orders_reviews as(
    select * 
    from {{ ref("fct_reviews") }}
),
customers as (
    select * 
    from {{ ref("stg_customers") }}
),
joined_tables as (
    select
        fct.order_id,
        fct.month_end_date,
        {{ datediff("fct.order_estimated_delivery_date", "fct.order_delivered_customer_date",  "day") }} as delivery_diff_days,
        fct.review_score,
        fct.review_score_desc
    from fact_orders_reviews fct
    left join customers cust
    on fct.customer_id = cust.customer_id
    where review_score_desc is not null
),
delivery_metrics as (
    select
        order_id,
        month_end_date,
        case
            when delivery_diff_days <= 0 then 'ahead of time delivery'
            when delivery_diff_days between 1 and 7 then 'delay 1-6 days'
            else 'delay >= 7 days'
        end as delivery_status,
        review_score,
        review_score_desc
    from joined_tables
),
results as (
    select 
        month_end_date,
        case
            when grouping(review_score_desc) = 1 then 'All'
            else coalesce(review_score_desc, 'N/A') -- avoid combining missing data and totals into a single bucket
        end as review_score_desc_display,
        case 
            when grouping(delivery_status) = 1 then 'All'
            else delivery_status
        end as delivery_status_display,
        count(order_id) as number_of_orders,
        round(avg(review_score), 2) as avg_review_score
    from delivery_metrics
    group by grouping sets(
        (month_end_date),
        (month_end_date, review_score_desc),
        (month_end_date, delivery_status)
    )
)
select 
    month_end_date,
    review_score_desc_display as review_score_desc,
    delivery_status_display as delivery_status,
    number_of_orders,
    avg_review_score
from results
