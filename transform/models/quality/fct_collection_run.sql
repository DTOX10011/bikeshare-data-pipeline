-- One row per source per collection run: what happened, how long it took, and how long it
-- had been since the previous run. This is where schedule gaps and failures show up.

with results as (

    select * from {{ ref('stg_run_log') }}

)

select
    md5(run_id || '|' || source || '|' || cast(started_at as varchar)) as run_source_key,
    run_id,
    started_at,
    finished_at,
    round(date_diff('millisecond', started_at, finished_at) / 1000.0, 3) as duration_seconds,
    source,
    status,
    bytes,
    snapshot_at,
    error,
    date_diff(
        'minute',
        lag(started_at) over (partition by source order by started_at),
        started_at
    ) as minutes_since_previous_run
from results
