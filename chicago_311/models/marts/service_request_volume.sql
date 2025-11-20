{{ config(materialized='table') }}

with base as (
    select * from {{ source('chicago', 'chicago_311_data') }}
)

select
    sr_type,
    count(*) as request_count
from base
group by sr_type
order by request_count desc
