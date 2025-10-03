{% macro to_date_utc(ts_col) -%}
  {# Cast timestamp to date (UTC assumption for warehouse) #}
  cast({{ ts_col }} as date)
{%- endmacro %}

{% macro hours_between(start_ts, end_ts) -%}
  {# Compute fractional hours between two timestamps #}
  cast((unix_timestamp({{ end_ts }}) - unix_timestamp({{ start_ts }})) / 3600.0 as double)
{%- endmacro %}
