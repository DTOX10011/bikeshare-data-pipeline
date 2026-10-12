-- Warns when, over the last 24 hours, too many rows report more bikes plus docks than the
-- station's capacity. The real feed does this now and then (by up to three over capacity in
-- the data collected so far), so only a larger share is treated as a problem.
{{ config(severity = 'warn') }}

with recent as (

    select status.num_bikes_available + status.num_docks_available > station.capacity as is_over_capacity
    from {{ ref('fct_station_status') }} as status
    join {{ ref('dim_station') }} as station using (station_id)
    where station.capacity > 0
      and status.snapshot_at between {{ quality_as_of() }} - interval 24 hour and {{ quality_as_of() }}

)

select
    count(*) as station_rows,
    count(*) filter (where is_over_capacity) as rows_over_capacity
from recent
having count(*) filter (where is_over_capacity) > {{ var('max_share_rows_over_capacity') }} * count(*)
