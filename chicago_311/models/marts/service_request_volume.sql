-- Service Request Volume Mart
with base as (
    select * from {{ source('chicago', 'chicago_311_data') }}
)
select
    date_trunc('month', created_date::timestamp) as month,
    count(*) as total_requests
from base
group by month
order by month
