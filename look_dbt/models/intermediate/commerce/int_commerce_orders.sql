{{ config(
  materialized='incremental',
  incremental_strategy='merge',
  unique_key='order_id',
  schema='silver_dev',
  partition_by=['order_date'],
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
  select * from {{ ref('int_commerce_orders__look') }}
  where canonical_updated_at >= dateadd(day, -{{ lookback_days }}, (select max_ts from tgt_max))
),
ranked as (
  select u.*,
         row_number() over (partition by order_id
                            order by canonical_updated_at desc,
                                     case source_system when 'look' then 1 else 99 end) as rn
  from unioned u
)
select
  order_id, user_id, order_status, order_date,
  created_at, shipped_at, delivered_at, returned_at,
  num_of_item, source_system,
  canonical_updated_at,
  canonical_updated_at as ingest_ts_utc
from ranked
where rn = 1;
