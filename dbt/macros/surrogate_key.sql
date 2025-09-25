{% macro sk(columns) %}
  -- Surrogate key helper (md5 of coalesced inputs)
  {{ dbt_utils.surrogate_key(columns) }}
{% endmacro %}
