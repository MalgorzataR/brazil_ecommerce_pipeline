{{
    config(
        materialized="view"
    )
}}

with customers as (
  select *,
    row_number() over (
        partition by 
            customer_id,
            customer_unique_id
        ) as rn
  from {{ source("staging", "customers") }}
)

select
    {{ dbt.safe_cast("customer_id", api.Column.translate_type("string")) }} as customer_id,
    {{ dbt.safe_cast("customer_unique_id", api.Column.translate_type("string")) }} as customer_unique_id,
    {{ dbt.safe_cast("customer_zip_code_prefix", api.Column.translate_type("string")) }} as customer_zip_code_prefix,
    {{ dbt.safe_cast("customer_city", api.Column.translate_type("string")) }} as customer_city,
    concat('BR-', {{ dbt.safe_cast("customer_state", api.Column.translate_type("string")) }}) as customer_state
from customers
where rn = 1
