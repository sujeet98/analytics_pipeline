{{ config(
  materialized = 'incremental',
  unique_key   = 'order_item_id',
  on_schema_change = 'append_new_columns'
) }}

-- Wide order item fact with product + DC context.
-- Incremental pruning via src_ingest_ts (from the order item).

select * from {{ ref('int_order_items_enriched') }}
{% if is_incremental() %}
where src_ingest_ts > coalesce(
  (select max(src_ingest_ts) from {{ this }}),
  timestamp'1970-01-01 00:00:00'
)
{% endif %}
