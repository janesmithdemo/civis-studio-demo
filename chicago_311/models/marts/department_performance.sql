-- Department Performance Mart
with source as (
    select * from {{ source('chicago', 'chicago_311_data') }}
)
select
    owner_department,
    count(*) as total_requests,
    sum(case when status = 'Completed' then 1 else 0 end) as completed_requests,
    sum(case when status = 'Open' then 1 else 0 end) as open_requests,
    sum(case when status = 'Canceled' then 1 else 0 end) as canceled_requests
from source
group by owner_department
