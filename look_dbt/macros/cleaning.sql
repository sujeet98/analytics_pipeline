{# ===========================================================
   Staging helpers: normalization & dedup
   Works on Databricks/Spark SQL
   =========================================================== #}

{# Trim whitespace; turn empty strings into NULL #}
{% macro clean_string(col) -%}
nullif(trim({{ col }}), '')
{%- endmacro %}

{# Safe timestamp cast #}
{% macro parse_ts(col) -%}
cast({{ col }} as timestamp)
{%- endmacro %}

{# Normalize ORDER status to a controlled set #}
{% macro normalize_order_status(col) -%}
case lower({{ col }})
  when 'created'     then 'created'
  when 'processing'  then 'processing'
  when 'shipped'     then 'shipped'
  when 'delivered'   then 'delivered'
  when 'returned'    then 'returned'
  when 'cancelled'   then 'cancelled'
  when 'complete'    then 'complete'
  else 'unknown'
end
{%- endmacro %}

{# Normalize EVENT type to a controlled set #}
{% macro normalize_event_type(col) -%}
case lower({{ col }})
  when 'pageview'         then 'pageview'
  when 'click'            then 'click'
  when 'purchase'         then 'purchase'
  when 'cancel'           then 'cancel'
  when 'add_to_cart'      then 'add_to_cart'
  when 'remove_from_cart' then 'remove_from_cart'
  when 'checkout'         then 'checkout'
  when 'login'            then 'login'
  when 'signup'           then 'signup'
  else 'unknown'
end
{%- endmacro %}

{# Cast money/float-like numbers to DECIMAL(18,2) #}
{% macro money_2(col) -%}
cast({{ col }} as decimal(18, 2))
{%- endmacro %}

{# Stable SHA256 for emails (lower + trim first) #}
{% macro email_sha256(col) -%}
sha2(lower(trim({{ col }})), 256)
{%- endmacro %}

{# Generic "keep latest per key" dedup.
   - relation: a ref() or source() expression already rendered in caller
   - key_cols:   list of business key columns (strings)
   - order_cols: list of columns to order by desc for latest
#}
{% macro deduplicate_latest(relation, key_cols, order_cols) -%}
with src as (
  select * from {{ relation }}
),
ranked as (
  select
    src.*,
    row_number() over (
      partition by {{ key_cols | join(', ') }}
      order by {{ order_cols | map('string') | join(' desc, ') }} desc
    ) as _rn
  from src
)
select * from ranked where _rn = 1
{%- endmacro %}
