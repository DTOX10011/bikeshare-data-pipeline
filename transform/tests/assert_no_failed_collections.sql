-- Warns about every source that failed in a collection run in the last 24 hours.
{{ config(severity = 'warn') }}

select run_id, started_at, source, error
from {{ ref('fct_collection_run') }}
where status = 'failed'
  and started_at between {{ quality_as_of() }} - interval 24 hour and {{ quality_as_of() }}
