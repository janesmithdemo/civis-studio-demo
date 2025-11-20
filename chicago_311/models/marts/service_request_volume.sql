-- Service Request Volume Mart
with base as (
    select * from {{ source('chicago', 'chicago_311_data') }}
)
select
    date_trunc('month', created_date::timestamp) as month,
    sr_type,
    count(*) as request_count
from base
group by month, sr_type
order by month desc, request_count desc
