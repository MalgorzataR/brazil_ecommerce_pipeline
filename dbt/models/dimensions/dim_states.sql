{{ config(materialized='table') }}

select
    state,
    iso_code,
    region
from {{ ref("brazil_states") }}