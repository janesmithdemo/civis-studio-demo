{{ config(materialized='table') }}

with base as (
    select * from {{ source('chicago', 'chicago_311_data') }}
)

select
    sr_number,
    created_date,
    closed_date,
    status,
    duplicate,
    (extract(epoch from closed_date::timestamp) - extract(epoch from created_date::timestamp))/3600 as hours_open
from base
where status = 'Open' or duplicate = true or ((extract(epoch from closed_date::timestamp) - extract(epoch from created_date::timestamp))/3600 > 72)
