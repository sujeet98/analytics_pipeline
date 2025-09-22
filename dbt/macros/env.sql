{% macro raw_source(name) %}
  {{ source('thelook_raw', name) }}
{% endmacro %}

{% macro bronze_cols() %}
  -- Standard system columns present in RAW → keep in bronze
  ingest_date,
  run_ts,
  ingest_ts_utc,
  source_table
{% endmacro %}