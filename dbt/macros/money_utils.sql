{% macro as_money_2(col) %}
  cast(round({{ col }}, 2) as decimal(18,2))
{% endmacro %}

{% macro safe_div(n, d) %}
  case when {{ d }} = 0 or {{ d }} is null then null else {{ n }} / {{ d }} end
{% endmacro %}
