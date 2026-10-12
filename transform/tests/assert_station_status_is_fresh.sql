-- Warns when the newest station snapshot is older than allowed: collection has stopped,
-- or the scheduler is running late. Also warns when no snapshot has been loaded at all.
{{ config(severity = 'warn') }}

select
    max(snapshot_at) as newest_snapshot_at,
    {{ quality_as_of() }} as checked_at
from {{ ref('fct_station_status') }}
having coalesce(max(snapshot_at), timestamp '1900-01-01')
     < {{ quality_as_of() }} - to_minutes({{ var('max_minutes_since_last_snapshot') }})
