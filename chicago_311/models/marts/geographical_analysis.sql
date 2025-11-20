{{ config(materialized='table') }}

with base as (
    select * from {{ source('chicago', 'chicago_311_data') }}
)

select
    community_area,
    ward,
    latitude,
    longitude,
    count(*) as request_count
from base
group by community_area, ward, latitude, longitude
order by request_count desc
