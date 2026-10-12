-- Warns when station rows were rejected in the last 24 hours (no station id, or no time).
{{ config(severity = 'warn') }}

select source_file, rejection_reason, count(*) as rejected_rows
from {{ ref('quarantine_station_status') }}
where partition_date >= cast({{ quality_as_of() }} - interval 24 hour as date)
group by all
