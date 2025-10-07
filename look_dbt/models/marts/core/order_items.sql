{{ config(
  materialized='incremental',
  unique_key='order_item_id',
  incremental_strategy='merge',
  on_schema_change='sync_all_columns'
) }}

with src as (
  select *
  from {{ ref('int_order_items_enriched') }}
  {% if is_incremental() %}
    -- prune source scan to recent changes for performance
    where src_ingest_ts >= (
      select coalesce(dateadd('day', -2, max(src_ingest_ts)), timestamp '1970-01-01')
      from {{ this }}
    )
  {% endif %}
)

select * from src
