{{ config(
  materialized='incremental',
  incremental_strategy='merge',
  unique_key='distribution_center_id',
  schema='silver_dev',
  partition_by=['_snap_date'],
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
  select * from {{ ref('int_commerce_distribution_centers__look') }}
  where canonical_updated_at >= dateadd(day, -{{ lookback_days }}, (select max_ts from tgt_max))
),
ranked as (
  select u.*,
         row_number() over (partition by distribution_center_id
                            order by canonical_updated_at desc,
                                     case source_system when 'look' then 1 else 99 end) as rn
  from unioned u
)
select
  distribution_center_id,
  name, latitude, longitude,
  _snap_date,
  source_system,
  canonical_updated_at,
  canonical_updated_at as ingest_ts_utc
from ranked
where rn = 1;
