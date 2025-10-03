{{
  config(
    materialized='incremental',
    unique_key='order_item_id',
    incremental_strategy='merge',
    partition_by={'field': 'created_date', 'data_type': 'date'}
  )
}}

-- Purpose: Entity table for order_items (denormalized with product attrs & costs)
-- Grain: 1 row per order_item_id

select * from {{ ref('int_order_items_enriched') }}

{% if is_incremental() %}
  -- Process only new / changed partitions by created_date
  where created_date >= (select coalesce(max(created_date), date'1900-01-01') from {{ this }})
{% endif %}
