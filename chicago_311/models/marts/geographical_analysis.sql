-- Geographical Analysis Mart
with base as (
    select * from {{ source('chicago', 'chicago_311_data') }}
)
select
    community_area,
    ward,
    count(*) as total_requests,
    avg(latitude) as avg_latitude,
    avg(longitude) as avg_longitude
from base
group by community_area, ward
order by total_requests desc
