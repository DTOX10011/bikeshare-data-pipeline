-- Warns about each gap between station snapshots in the last 24 hours that is longer than
-- allowed. Every gap means a stretch of time with no data, which no later run can recover.
{{ config(severity = 'warn') }}

with recent_snapshots as (

    select distinct snapshot_at
    from {{ ref('fct_station_status') }}
    where snapshot_at between {{ quality_as_of() }} - interval 24 hour and {{ quality_as_of() }}

),

gaps as (

    select
        lag(snapshot_at) over (order by snapshot_at) as previous_snapshot_at,
        snapshot_at,
        date_diff('minute', lag(snapshot_at) over (order by snapshot_at), snapshot_at) as gap_minutes
    from recent_snapshots

)

select *
from gaps
where gap_minutes > {{ var('max_minutes_between_snapshots') }}
