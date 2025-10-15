{{ config(
  materialized = 'incremental',
  incremental_strategy = 'insert_overwrite',
  partition_by = ['session_date'],
  unique_key = ['session_date','traffic_source'],
  on_schema_change = 'sync_all_columns',
  file_format = 'delta'
) }}

{% set lookback_days = var('funnel_lookback_days', 2) %}
{% set attribution_hours = var('funnel_attribution_hours', 24) %}

with
{% if is_incremental() %}
-- Rebuild window for incremental runs
last_built as (
  select coalesce(max(session_date), date'1900-01-01') as max_session_date
  from {{ this }}
),
cutoffs as (
  select date_add(max_session_date, -{{ lookback_days }}) as min_session_date_rebuild
  from last_built
),
{% endif %}

-- 1) Normalize events (lowercase event_type, stable order key)
ev_source as (
  select
    session_id,
    customer_sk,
    user_id,
    coalesce(sequence_number, 999999) as seq,
    event_ts,
    event_date,
    traffic_source,
    uri,
    lower(event_type) as event_type
  from {{ ref('fact_events_current') }}
  {% if is_incremental() %}
    where event_date >= (select min_session_date_rebuild from cutoffs)
  {% endif %}
),

-- 2) Session time bounds
session_bounds as (
  select
    session_id,
    customer_sk,
    min(event_ts) as session_start_ts,
    max(event_ts) as session_end_ts
  from ev_source
  group by session_id, customer_sk
),

-- 3) First observed session dims by (seq, ts) -- only traffic_source now
session_first_dims as (
  select
    session_id,
    element_at(
      array_sort(collect_list(named_struct('seq', seq, 'ts', event_ts, 'v', traffic_source))), 1
    ).v as session_traffic_source
  from ev_source
  group by session_id
),

-- 4) Funnel flags using your event types
funnel_flags as (
  select
    session_id,
    max(case when event_type = 'home'       then 1 else 0 end) as f_home_event,
    max(case when event_type = 'department' then 1 else 0 end) as f_department_event,
    max(case when event_type = 'product'    then 1 else 0 end) as f_product_event,
    max(case when event_type = 'cart'       then 1 else 0 end) as f_cart_event,
    max(case when event_type = 'purchase'   then 1 else 0 end) as f_purchase_event,
    max(case when event_type = 'cancel'     then 1 else 0 end) as f_cancel_event
  from ev_source
  group by session_id
),

-- 5) Session rows (partition driver = session_date)
sessions as (
  select
    b.session_id,
    b.customer_sk,
    to_date(b.session_start_ts) as session_date,
    b.session_start_ts,
    b.session_end_ts,
    coalesce(d.session_traffic_source, 'unknown') as traffic_source
  from session_bounds b
  left join session_first_dims d
    on d.session_id = b.session_id
  {% if is_incremental() %}
    where to_date(b.session_start_ts) >= (select min_session_date_rebuild from cutoffs)
  {% endif %}
),

-- (Optional) fallback to dim_customer traffic_source if session-level is null
cust_fallback as (
  select customer_sk, traffic_source as cust_traffic_source
  from {{ ref('dim_customer_current') }}
),

-- 6) Orders near sessions (apply your status policy here). Normalize status to lower and exclude non-realized orders.
ord_source as (
  select
    order_id,
    customer_sk,
    created_at,
    lower(coalesce(order_status,'unknown')) as order_status_lc
  from {{ ref('fact_orders_current') }}
  where lower(coalesce(order_status,'unknown')) not in ('cancelled','void','failed','refunded','returned')
  {% if is_incremental() %}
    and to_date(created_at) >= (select min_session_date_rebuild from cutoffs)
  {% endif %}
),

-- 7) Map ALL orders to sessions by customer & time window (no de-dupe)
sess_orders as (
  select
    s.session_id,
    o.order_id,
    o.created_at as order_ts
  from sessions s
  join ord_source o
    on o.customer_sk = s.customer_sk
   and o.created_at >= s.session_start_ts
   and o.created_at <= s.session_end_ts + interval {{ attribution_hours }} hours
),

-- 8) Order GMV at order grain
order_revenue as (
  select
    o.order_id,
    sum(coalesce(oi.sale_price, 0)) as order_gmv
  from {{ ref('fact_orders_current') }} o
  left join {{ ref('fact_order_items_current') }} oi
    on oi.order_id = o.order_id
  group by o.order_id
),

-- 9) Aggregate to session: count all orders and sum their GMV
orders_per_session as (
  select
    so.session_id,
    count(*) as orders_count,
    sum(coalesce(orv.order_gmv, 0)) as session_gmv
  from sess_orders so
  left join order_revenue orv
    on orv.order_id = so.order_id
  group by so.session_id
),

-- 10) Session-level fact (multiple orders supported)
int_session_funnel as (
  select
    s.session_id,
    s.session_date,
    s.customer_sk,
    s.session_start_ts,
    s.session_end_ts,
    coalesce(s.traffic_source, cf.cust_traffic_source, 'unknown') as traffic_source,
    f.f_home_event,
    f.f_department_event,
    f.f_product_event,
    f.f_cart_event,
    f.f_purchase_event,
    f.f_cancel_event,
    coalesce(ops.orders_count, 0) as orders_count,
    coalesce(ops.session_gmv, 0) as session_gmv
  from sessions s
  left join funnel_flags f   on f.session_id = s.session_id
  left join orders_per_session ops on ops.session_id = s.session_id
  left join cust_fallback cf on cf.customer_sk = s.customer_sk
)

-- 11) Final daily/channel rollup (session_date × traffic_source)
select
  session_date,
  coalesce(traffic_source, 'unknown') as traffic_source,

  count(*) as sessions,

  -- step reach rates
  avg(case when f_home_event       = 1 then 1.0 else 0.0 end) as step_home_rate,
  avg(case when f_department_event = 1 then 1.0 else 0.0 end) as step_department_rate,
  avg(case when f_product_event    = 1 then 1.0 else 0.0 end) as step_product_rate,
  avg(case when f_cart_event       = 1 then 1.0 else 0.0 end) as step_cart_rate,
  avg(case when f_purchase_event   = 1 then 1.0 else 0.0 end) as step_purchase_rate,
  avg(case when f_cancel_event     = 1 then 1.0 else 0.0 end) as step_cancel_rate,

  -- Conditional pass-throughs (session-based)
  case when sum(case when f_product_event = 1 then 1 else 0 end) > 0
       then sum(case when f_cart_event    = 1 then 1 else 0 end) * 1.0
          / sum(case when f_product_event = 1 then 1 else 0 end)
       else null end as atc_from_product_rate,

  case when sum(case when f_cart_event = 1 then 1 else 0 end) > 0
       then sum(case when f_purchase_event = 1 then 1 else 0 end) * 1.0
          / sum(case when f_cart_event     = 1 then 1 else 0 end)
       else null end as purchase_click_from_atc_rate,

  -- Conversion is session-based: % of sessions with >= 1 order
  avg(case when orders_count > 0 then 1.0 else 0.0 end) as conversion_rate,

  -- Among ATC sessions, share that resulted in >= 1 order (session-based)
  case when sum(case when f_cart_event = 1 then 1 else 0 end) > 0
       then sum(case when f_cart_event = 1 and orders_count > 0 then 1 else 0 end) * 1.0
          / sum(case when f_cart_event = 1 then 1 else 0 end)
       else null end as purchase_actual_from_atc_rate,

  -- Revenue: sum of all orders mapped to sessions
  sum(session_gmv) as gmv,

  -- AOV across ALL orders (not just one per session)
  case when sum(orders_count) > 0
       then sum(session_gmv) * 1.0 / sum(orders_count)
       else null end as aov
from int_session_funnel
group by session_date, traffic_source
{% if is_incremental() %}
having session_date >= (select min_session_date_rebuild from cutoffs)
{% endif %}
;
