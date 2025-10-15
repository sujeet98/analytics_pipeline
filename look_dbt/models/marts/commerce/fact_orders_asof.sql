{{ config(
  materialized='incremental',
  incremental_strategy='merge',
  unique_key='order_id',
  partition_by=['order_date'],
  cluster_by=['customer_sk'],
  tags=['marts','fact','asof']
) }}

{% set rewindow_days = 21 %}

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
),

cust_first as (
  select global_customer_id, min(valid_from) as first_valid_from
  from {{ ref('dim_customer') }} group by 1
),
cust_earliest as (
  select d.*
  from {{ ref('dim_customer') }} d
  join cust_first f
    on d.global_customer_id = f.global_customer_id
   and d.valid_from = f.first_valid_from
),

cust as (
  select
    b.*,
    coalesce(dc.customer_sk, dce.customer_sk) as customer_sk
  from base b
  left join {{ ref('dim_customer') }} dc
    on dc.global_customer_id = concat(b.source_system, ':', cast(b.user_id as string))
   and b.created_at >= dc.valid_from
   and b.created_at <  dc.valid_to
  left join cust_earliest dce
    on dce.global_customer_id = concat(b.source_system, ':', cast(b.user_id as string))
   and dc.customer_sk is null
)

select
  order_id,
  customer_sk,
  order_status,
  order_date,
  created_at, shipped_at, delivered_at, returned_at,
  num_of_item
from cust;
