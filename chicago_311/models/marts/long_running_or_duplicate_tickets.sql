-- Long Running or Duplicate Tickets Mart
with base as (
    select * from {{ source('chicago', 'chicago_311_data') }}
)
select
    sr_number,
    created_date,
    closed_date,
    status,
    duplicate,
    (closed_date::timestamp - created_date::timestamp) as duration_days
from base
where (closed_date is not null and (closed_date::timestamp - created_date::timestamp) > interval '30 days')
   or duplicate = true
order by duration_days desc nulls last
