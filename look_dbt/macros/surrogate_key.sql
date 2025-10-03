{% macro sk(columns) %}
  {# Creates a stable md5 surrogate key from a list of columns.
     Usage: {{ sk(['order_id','user_id']) }} #}
  {{ dbt_utils.surrogate_key(columns) }}
{% endmacro %}
