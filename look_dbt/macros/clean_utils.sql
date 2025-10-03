-- Common cleansing helpers used in staging models.
-- These handle typical "dirty" inputs like "-" or empty strings.

{% macro clean_ts(col) -%}
  {# 
  Normalize to NULL and cast to timestamp.
  - Databricks/Spark will parse ISO8601 strings nicely.
  #}
  try_cast(nullif(nullif(trim(cast({{ col }} as string)), '-'), '') as timestamp)
{%- endmacro %}

{% macro clean_str(col) -%}
  {# Trim and convert empty to NULL #}
  nullif(trim({{ col }}), '')
{%- endmacro %}

{% macro clean_lower(col) -%}
  {# Lowercase trimmed string for normalization #}
  lower(trim({{ col }}))
{%- endmacro %}

{% macro normalize_status(col) -%}
  {# Normalize status to a controlled set common in ecommerce #}
  case lower(trim({{ col }}))
    when 'complete'   then 'complete'
    when 'completed'  then 'complete'
    when 'shipped'    then 'shipped'
    when 'processing' then 'processing'
    when 'cancelled'  then 'cancelled'
    when 'returned'   then 'returned'
    else lower(trim({{ col }}))
  end
{%- endmacro %}
