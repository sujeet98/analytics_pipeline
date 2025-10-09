{{ config(
  materialized='incremental',
  incremental_strategy='merge',      
  unique_key='event_id',
  schema='silver_dev',
  partition_by=['event_date'],
  on_schema_change='sync_all_columns',
  tags=['intermediate','commerce']
) }}

{% set lookback_days = 2 %}

with tgt_max as (
  select
    {% if is_incremental() %} coalesce(max(canonical_updated_at), timestamp('1900-01-01'))
    {% else %}                 timestamp('1900-01-01') {% endif %} as max_ts
  from {% if is_incremental() %} {{ this }} {% else %} (select 1) _ {% endif %}
),
unioned as (
  select * from {{ ref('int_commerce_events__look') }}
  where canonical_updated_at >= dateadd(day, -{{ lookback_days }}, (select max_ts from tgt_max))
)
select * from unioned;
