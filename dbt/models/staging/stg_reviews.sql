{{
    config(
        materialized="view"
    )
}} 
with reviews as (
  select *,
    row_number() over (
        partition by 
            review_id,
            order_id
        ) as rn
  from {{ source("staging", "order_reviews") }}
)

select
    {{ dbt.safe_cast("review_id", api.Column.translate_type("string")) }} as review_id,
    {{ dbt.safe_cast("order_id", api.Column.translate_type("string")) }} as order_id,
    {{ dbt.safe_cast("review_score", api.Column.translate_type("integer")) }} as review_score,
    {{ get_review_score_description("review_score") }} as review_score_desc,
    {{ dbt.safe_cast("review_creation_date", api.Column.translate_type("timestamp")) }} as review_creation_date,
    {{ dbt.safe_cast("review_answer_timestamp", api.Column.translate_type("timestamp")) }} as review_answer_timestamp
from reviews
where rn = 1