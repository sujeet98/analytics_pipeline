{{
  config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='merge',
    partition_by={'field': 'created_date', 'data_type': 'date'}
  )
}}

-- Purpose: Entity table for orders aggregated from items with SLA metrics
-- Grain: 1 row per order_id

select * from {{ ref('int_orders_aggregated_from_items') }}

{% if is_incremental() %}
  where created_date >= (select coalesce(max(created_date), date'1900-01-01') from {{ this }})
{% endif %}
