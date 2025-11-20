-- Service Request Volume Mart
with source as (
    select * from {{ source('chicago', 'chicago_311_data') }}
)
select
    created_month,
    created_day_of_week,
    count(*) as request_count
from source
group by created_month, created_day_of_week
