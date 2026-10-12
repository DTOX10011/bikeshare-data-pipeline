-- Warns when, over the last 24 hours, more than a small share of station rows had a field
-- whose value could not be read. A few are noise; many usually mean the feed changed format.
{{ config(severity = 'warn') }}

with recent as (

    select unreadable_value_count
    from {{ ref('fct_station_status') }}
    where snapshot_at between {{ quality_as_of() }} - interval 24 hour and {{ quality_as_of() }}

)

select
    count(*) as station_rows,
    count(*) filter (where unreadable_value_count > 0) as rows_with_unreadable_values
from recent
having count(*) filter (where unreadable_value_count > 0)
     > {{ var('max_share_rows_with_unreadable_values') }} * count(*)
