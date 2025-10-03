{% macro as_money_2(col) -%}
  {# Round to 2 decimal places & cast to decimal (helps avoid float artifacts) #}
  cast(round({{ col }}, 2) as decimal(18,2))
{%- endmacro %}

{% macro safe_div(n, d) -%}
  {# Avoid divide-by-zero by returning NULL when denominator is 0 or NULL #}
  case when {{ d }} = 0 or {{ d }} is null then null else {{ n }} / {{ d }} end
{%- endmacro %}
