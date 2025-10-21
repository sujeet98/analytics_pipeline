{{ config(
  materialized = 'incremental',
  incremental_strategy = 'merge',
  partition_by = ['session_date'],
  unique_key = ['session_date','traffic_source'],
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

-- 3) First observed session traffic_source by (seq, ts)
session_first_dims as (
  select
    session_id,
    element_at(
      array_sort(collect_list(named_struct('seq', seq, 'ts', event_ts, 'v', traffic_source))), 1
    ).v as session_traffic_source
  from ev_source
  group by session_id
),

-- 4) Funnel flags
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

-- Optional fallback to dim_customer traffic_source if session-level is null
cust_fallback as (
  select customer_sk, traffic_source as cust_traffic_source
  from {{ ref('dim_customer_current') }}
),

/* =====================  ORDER → SESSION ATTRIBUTION (strict)  =====================
   Rule: exactly ONE session per order, requiring purchase intent on the session.
   Tier A: order_ts inside session [start, end] → choose closest to session_end_ts
           (then latest session_start_ts, then greatest session_id)
   Tier B: if none, choose latest session that ended within {{ attribution_hours }}h BEFORE order_ts
   ================================================================================ */

-- sessions eligible to own orders (must have purchase intent + known customer)
sess as (
  select
    s.session_id,
    s.customer_sk,
    s.session_start_ts,
    s.session_end_ts,
    s.traffic_source
  from sessions s
  join funnel_flags f on f.session_id = s.session_id
  where s.customer_sk is not null
    and coalesce(f.f_purchase_event, 0) = 1
  {% if is_incremental() %}
    and to_date(s.session_start_ts) >= (select min_session_date_rebuild from cutoffs)
  {% endif %}
),

-- qualifying orders (status filtered)
orders as (
  select
    o.order_id,
    o.customer_sk,
    cast(o.created_at as timestamp) as order_ts
  from {{ ref('fact_orders_current') }} o
  where lower(coalesce(o.order_status,'unknown')) not in ('cancelled','void','failed','refunded','returned')
  {% if is_incremental() %}
    and to_date(o.created_at) >= (select min_session_date_rebuild from cutoffs)
  {% endif %}
),

-- Tier A: in-window candidates
tier_a as (
  select
    o.order_id,
    s.session_id,
    abs(unix_timestamp(o.order_ts) - unix_timestamp(s.session_end_ts)) as dist_to_end,
    s.session_start_ts,
    s.session_id as sid
  from orders o
  join sess s
    on s.customer_sk = o.customer_sk
   and s.session_start_ts <= o.order_ts
   and o.order_ts       <= s.session_end_ts
),

pick_a as (
  select order_id, session_id
  from (
    select
      order_id,
      session_id,
      row_number() over (
        partition by order_id
        order by dist_to_end asc, session_start_ts desc, sid desc
      ) as rn
    from tier_a
  ) x
  where rn = 1
),

-- Tier B: latest session that ended within the lookback BEFORE the order
tier_b as (
  select
    o.order_id,
    s.session_id,
    s.session_end_ts,
    s.session_id as sid
  from orders o
  join sess s
    on s.customer_sk = o.customer_sk
   and s.session_end_ts <  o.order_ts
   and s.session_end_ts >= o.order_ts - interval {{ attribution_hours }} hours
  left join pick_a a on a.order_id = o.order_id
  where a.order_id is null
),

pick_b as (
  select order_id, session_id
  from (
    select
      order_id,
      session_id,
      row_number() over (
        partition by order_id
        order by session_end_ts desc, sid desc
      ) as rn
    from tier_b
  ) x
  where rn = 1
),

-- one chosen session per order (or null)
order_to_session as (
  select
    o.order_id,
    coalesce(a.session_id, b.session_id) as session_id
  from orders o
  left join pick_a a using (order_id)
  left join pick_b b using (order_id)
),

-- attributed orders per session (no double counting)
sess_orders as (
  select
    s.session_id,
    o.order_id,
    o.order_ts
  from order_to_session ots
  join orders o      on o.order_id   = ots.order_id
  left join sess s   on s.session_id = ots.session_id
  where ots.session_id is not null
),

-- order GMV
order_revenue as (
  select
    o.order_id,
    sum(coalesce(oi.sale_price, 0)) as order_gmv
  from {{ ref('fact_orders_current') }} o
  left join {{ ref('fact_order_items_current') }} oi
    on oi.order_id = o.order_id
  group by o.order_id
),

-- per-session aggregates (strictly single-attributed orders)
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

-- 10) Session-level fact (atomic)
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

  -- additive facts
  count(*)                          as sessions,
  sum(orders_count)                 as orders_count,
  sum(session_gmv)                  as gmv,

  -- step rates (session-based)
  avg(case when f_home_event       = 1 then 1.0 else 0.0 end) as step_home_rate,
  avg(case when f_department_event = 1 then 1.0 else 0.0 end) as step_department_rate,
  avg(case when f_product_event    = 1 then 1.0 else 0.0 end) as step_product_rate,
  avg(case when f_cart_event       = 1 then 1.0 else 0.0 end) as step_cart_rate,
  avg(case when f_purchase_event   = 1 then 1.0 else 0.0 end) as step_purchase_rate,
  avg(case when f_cancel_event     = 1 then 1.0 else 0.0 end) as step_cancel_rate,

  -- conditional pass-throughs (session-based)
  case when sum(case when f_product_event = 1 then 1 else 0 end) > 0
       then sum(case when f_cart_event    = 1 then 1 else 0 end) * 1.0
            / sum(case when f_product_event = 1 then 1 else 0 end)
       else null end as atc_from_product_rate,

  case when sum(case when f_cart_event = 1 then 1 else 0 end) > 0
       then sum(case when f_purchase_event = 1 then 1 else 0 end) * 1.0
            / sum(case when f_cart_event     = 1 then 1 else 0 end)
       else null end as purchase_click_from_atc_rate,

  -- conversion is session-based: % sessions with >=1 order
  avg(case when orders_count > 0 then 1.0 else 0.0 end) as conversion_rate,

  -- among ATC sessions, share with >=1 order (session-based)
  case when sum(case when f_cart_event = 1 then 1 else 0 end) > 0
       then sum(case when f_cart_event = 1 and orders_count > 0 then 1 else 0 end) * 1.0
            / sum(case when f_cart_event = 1 then 1 else 0 end)
       else null end as purchase_actual_from_atc_rate,

  -- AOV across all attributed orders
  case when sum(orders_count) > 0
       then sum(session_gmv) * 1.0 / sum(orders_count)
       else null end as aov

from int_session_funnel
group by session_date, traffic_source
{% if is_incremental() %}
having session_date >= (select min_session_date_rebuild from cutoffs)
{% endif %}
;
