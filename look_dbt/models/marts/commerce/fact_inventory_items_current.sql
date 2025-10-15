{{ config(
  materialized='incremental',
  incremental_strategy='merge',
  unique_key='inventory_item_id',
  partition_by=['created_date'],
  cluster_by=['product_sk','dc_sk'],
  tags=['marts','fact','current']
) }}

{% set rewindow_days = 7 %}

with base as (
  select
    source_system,
    inventory_item_id,
    product_id,
    distribution_center_id,
    created_at,
    created_date,
    sold_at,
    unit_cost,
    retail_price
  from {{ ref('core_commerce_inventory_items') }}
  {% if is_incremental() %}
    where created_date >= dateadd(day, -{{ rewindow_days }}, current_date())
  {% endif %}
),

prod as (
  select
    b.*,
    dp.product_sk,
    coalesce(dp.distribution_center_id, b.distribution_center_id) as product_dc_id
  from base b
  left join {{ ref('dim_product_current') }} dp
    on dp.global_product_id = concat(b.source_system, ':', cast(b.product_id as string))
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
  inventory_item_id,
  product_sk,
  dc_sk,
  created_at,
  created_date,
  sold_at,
  unit_cost,
  retail_price
from dcj;
