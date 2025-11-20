-- Geographical Analysis Mart
with source as (
    select * from {{ source('chicago', 'chicago_311_data') }}
)
select
    community_area,
    ward,
    latitude,
    longitude,
    count(*) as request_count
from source
group by community_area, ward, latitude, longitude
