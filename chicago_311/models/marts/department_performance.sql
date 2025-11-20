-- Department Performance Mart
with base as (
    select * from {{ source('chicago', 'chicago_311_data') }}
)
select
    owner_department,
    count(*) as total_requests,
    sum(case when status = 'Completed' then 1 else 0 end) as completed_requests,
    sum(case when status = 'Open' then 1 else 0 end) as open_requests,
    avg(extract(epoch from (closed_date::timestamp - created_date::timestamp))/3600) as avg_hours_to_close
from base
group by owner_department
order by total_requests desc
