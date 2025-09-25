-- Common cleansing helpers used in staging
{% macro clean_ts(col) %}
  -- Normalize "-" / empty to NULL, then cast to timestamp
  try_cast(nullif(nullif(trim(cast({{ col }} as string)), '-'), '') as timestamp)
{% endmacro %}

{% macro clean_str(col) %}
  nullif(trim({{ col }}), '')
{% endmacro %}

{% macro clean_lower(col) %}
  lower(trim({{ col }}))
{% endmacro %}

{% macro normalize_status(col) %}
  case lower(trim({{ col }}))
    when 'complete' then 'complete'
    when 'shipped' then 'shipped'
    when 'processing' then 'processing'
    when 'cancelled' then 'cancelled'
    when 'returned' then 'returned'
    else lower(trim({{ col }}))
  end
{% endmacro %}
