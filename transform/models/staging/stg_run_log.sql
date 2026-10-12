-- One row per source per collection run, read from the ingest job's run log.

with runs as (

    select
        run_id,
        started_at,
        finished_at,
        unnest(sources) as result
    from read_json(
        {{ run_log_files() }},
        format = 'newline_delimited',
        columns = {'run_id': 'VARCHAR', 'started_at': 'VARCHAR', 'finished_at': 'VARCHAR', 'sources': 'JSON[]'}
    )

)

select
    run_id,
    {{ iso_text_to_utc_timestamp('started_at') }} as started_at,
    {{ iso_text_to_utc_timestamp('finished_at') }} as finished_at,
    result ->> 'source' as source,
    result ->> 'status' as status,
    try_cast(result ->> 'bytes' as bigint) as bytes,
    {{ iso_text_to_utc_timestamp("result ->> 'snapshot_time'") }} as snapshot_at,
    result ->> 'error' as error
from runs
