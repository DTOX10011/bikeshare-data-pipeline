-- Warns about rows that report more e-bikes than bikes. The feed's bike count includes
-- e-bikes, so this cannot be true and points at a feed error.
{{ config(severity = 'warn') }}

select station_id, snapshot_at, num_bikes_available, num_ebikes_available
from {{ ref('fct_station_status') }}
where num_ebikes_available > num_bikes_available
