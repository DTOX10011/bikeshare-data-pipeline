-- Every station row of every raw station_status file, untyped. The two models below read
-- from this view: one keeps the usable rows, the other keeps the rows that cannot be used.

select
    dt as partition_date,
    regexp_extract(filename, '[^/\\]+$') as source_file,
    {{ unix_seconds_to_timestamp('last_updated') }} as snapshot_at,
    unnest(data.stations) as station
from read_json(
    {{ raw_files('station_status') }},
    columns = {'last_updated': 'BIGINT', 'data': 'STRUCT(stations JSON[])'},
    hive_partitioning = true,
    filename = true
)
