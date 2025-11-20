-- Long Running or Duplicate Tickets Mart
with source as (
    select * from {{ source('chicago', 'chicago_311_data') }}
)
select
    sr_number,
    created_date,
    closed_date,
    status,
    duplicate,
    legacy_record,
    owner_department,
    (case when closed_date is not null and created_date is not null then datediff(day, created_date::date, closed_date::date) else null end) as days_open
from source
where duplicate = true or (closed_date is not null and created_date is not null and datediff(day, created_date::date, closed_date::date) > 30)
