-- Department Performance Mart
with base as (
    select * from {{ source('chicago', 'chicago_311_data') }}
)
select
    owner_department,
    count(*) as total_requests,
    count(case when status = 'Completed' then 1 end) as completed_requests,
    count(case when status = 'Open' then 1 end) as open_requests,
    min(created_date) as first_request_date,
    max(created_date) as last_request_date
from base
group by owner_department
order by total_requests desc
