{{
    config(
        materialized = "table"
    )
}}
with dates as (
    {{ dbt_date.get_date_dimension("2016-01-01", "2018-08-31") }}
)

select 
    *,
    {{ dbt_utils.generate_surrogate_key(['date(date_day)']) }} as date_id
from dates