-- Long Running or Duplicate Tickets Mart
with base as (
    select * from {{ source('chicago', 'chicago_311_data') }}
)
select
    sr_number,
    status,
    created_date,
    closed_date,
    duplicate,
    owner_department,
    sr_type,
    (extract(epoch from (closed_date::timestamp - created_date::timestamp))/3600) as hours_open
from base
where duplicate = true
   or (status = 'Open' or (closed_date is not null and (extract(epoch from (closed_date::timestamp - created_date::timestamp))/3600) > 168))
order by hours_open desc nulls last
