{% macro email_sha256(col) %}
  sha2(lower(trim({{ col }})), 256)
{% endmacro %}
