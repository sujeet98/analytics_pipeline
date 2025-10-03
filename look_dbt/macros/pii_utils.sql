{% macro email_sha256(col) -%}
  {# Lowercase, trim, then SHA-256 hash (PII-safe join/comparison) #}
  sha2(lower(trim({{ col }})), 256)
{%- endmacro %}
