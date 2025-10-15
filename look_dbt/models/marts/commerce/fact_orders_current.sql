{{ config(
  materialized='incremental',
  incremental_strategy='merge',
  unique_key='order_id',
  partition_by=['order_date'],
  cluster_by=['customer_sk'],
  tags=['marts','fact','current']
) }}

{% set rewindow_days = 7 %}

with base as (
  select
    source_system,
    order_id,
    user_id,
    order_status,
    order_date,
    created_at, shipped_at, delivered_at, returned_at,
    num_of_item
  from {{ ref('core_commerce_orders') }}
  {% if is_incremental() %}
    where order_date >= dateadd(day, -{{ rewindow_days }}, current_date())
  {% endif %}
)

select
  b.order_id,
  dc.customer_sk,
  b.order_status,
  b.order_date,
  b.created_at, b.shipped_at, b.delivered_at, b.returned_at,
  b.num_of_item
from base b
left join {{ ref('dim_customer_current') }} dc
  on dc.global_customer_id = concat(b.source_system, ':', cast(b.user_id as string));
