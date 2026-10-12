-- Station rows that cannot be used: no station id, or a snapshot with no readable time.
-- They are kept, with the reason, so they can be counted and inspected instead of vanishing.

select
    partition_date,
    source_file,
    snapshot_at,
    case
        when snapshot_at is null then 'snapshot has no readable time'
        else 'row has no station_id'
    end as rejection_reason,
    cast(station as varchar) as raw_row
from {{ ref('base_station_status_rows') }}
where nullif(station ->> 'station_id', '') is null
   or snapshot_at is null
