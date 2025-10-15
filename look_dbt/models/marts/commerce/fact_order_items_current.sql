{{ config(
  materialized='incremental',
  incremental_strategy='merge',
  unique_key='order_item_id',
  partition_by=['created_date'],
  cluster_by=['customer_sk','product_sk','dc_sk'],
  tags=['marts','fact','current']
) }}

{% set rewindow_days = 7 %}

with base as (
  select
    source_system,
    order_item_id,
    order_id,
    user_id,
    product_id,
    created_at,
    created_date,
    item_date,
    shipped_at, delivered_at, returned_at,
    sale_price
  from {{ ref('core_commerce_order_items') }}
  {% if is_incremental() %}
    where created_date >= dateadd(day, -{{ rewindow_days }}, current_date())
  {% endif %}
),

cust as (
  select
    b.*,
    dc.customer_sk
  from base b
  left join {{ ref('dim_customer_current') }} dc
    on dc.global_customer_id = concat(b.source_system, ':', cast(b.user_id as string))
),

prod as (
  select
    c.*,
    dp.product_sk,
    dp.distribution_center_id as product_dc_id
  from cust c
  left join {{ ref('dim_product_current') }} dp
    on dp.global_product_id = concat(c.source_system, ':', cast(c.product_id as string))
),

dcj as (
  select
    p.*,
    ddc.dc_sk
  from prod p
  left join {{ ref('dim_distribution_center_current') }} ddc
    on ddc.global_dc_id = concat(p.source_system, ':', cast(p.product_dc_id as string))
)

select
  order_item_id,
  order_id,
  customer_sk,
  product_sk,
  dc_sk,
  created_at,
  created_date,
  item_date,
  shipped_at, delivered_at, returned_at,
  sale_price
from dcj;
