{{ config(
    materialized = 'table',
    tags = ['mart','leak','growth'],
) }}

{%- set LOOKBACK_DAYS = var('leak_lookback_days', 1000) -%}
{%- set MIN_SESSIONS  = var('leak_min_sessions', 3) -%}
{%- set DROP_FRAC     = var('leak_drop_threshold', 0.25) -%}

with
-- 1) Limit to the analysis window
sessions_base as (
  select *
  from {{ ref('mart_session_funnel') }}
  where session_date >= date_add(current_date(), -{{ LOOKBACK_DAYS }})
),

-- 2) Derive per-session attributes from events (modal state per session)
event_attrs as (
  select
    session_id,
    state,
    count(*) as cnt
  from {{ ref('fact_events_current') }}
  where event_date >= date_add(current_date(), -{{ LOOKBACK_DAYS }})
    and session_id is not null
  group by session_id, state
),
event_attrs_ranked as (
  select
    session_id,
    state,
    row_number() over (
      partition by session_id
      order by cnt desc, state asc
    ) as rn
  from event_attrs
),
session_dims as (
  select
    session_id,
    coalesce(state, 'Unknown') as state
  from event_attrs_ranked
  where rn = 1
),

-- 3) Enrich sessions with attributes
sessions_enriched as (
  select
    s.session_id,
    s.session_date,
    s.traffic_source,
    coalesce(d.state, 'Unknown') as state,
    -- step flags & realized orders
    s.f_product_event,
    s.f_cart_event,
    s.orders_count
  from sessions_base s
  left join session_dims d
    on d.session_id = s.session_id
),

-- 4) Aggregate to segments: channel × geo (state)
segment_rollup as (
  select
    traffic_source,
    state,
    count(*) as sessions,
    sum(case when f_product_event = 1 then 1 else 0 end)                                   as product_sessions,
    sum(case when f_cart_event    = 1 then 1 else 0 end)                                   as atc_sessions,
    sum(case when f_product_event = 1 and f_cart_event = 1 then 1 else 0 end)              as atc_given_product_sessions,
    sum(case when f_cart_event = 1 and orders_count > 0 then 1 else 0 end)                 as purchases_given_atc_sessions,

    -- Step rates
    (sum(case when f_product_event = 1 and f_cart_event = 1 then 1 else 0 end)
     / nullif(sum(case when f_product_event = 1 then 1 else 0 end), 0))                    as product_to_cart_rate,

    (sum(case when f_cart_event = 1 and orders_count > 0 then 1 else 0 end)
     / nullif(sum(case when f_cart_event = 1 then 1 else 0 end), 0))                       as cart_to_purchase_rate
  from sessions_enriched
  group by 1, 2
),

-- 5) Compute medians across comparable segments (only using sufficiently large segments)
global_medians as (
  select
    percentile_approx(product_to_cart_rate, 0.5)
      filter (where sessions >= {{ MIN_SESSIONS }}) as median_product_to_cart,
    percentile_approx(cart_to_purchase_rate, 0.5)
      filter (where sessions >= {{ MIN_SESSIONS }}) as median_cart_to_purchase
  from segment_rollup
),

-- 6) Compare each segment to medians & flag leaks
scored as (
  select
    r.*,
    m.median_product_to_cart,
    m.median_cart_to_purchase,

    case
      when m.median_product_to_cart is null then null
      when r.product_to_cart_rate   is null then null
      else r.product_to_cart_rate / m.median_product_to_cart
    end as product_to_cart_vs_median,

    case
      when m.median_cart_to_purchase is null then null
      when r.cart_to_purchase_rate   is null then null
      else r.cart_to_purchase_rate / m.median_cart_to_purchase
    end as cart_to_purchase_vs_median,

    -- Leak flags: rate < (1 - DROP_FRAC) * median  AND segment size threshold
    case
      when sessions < {{ MIN_SESSIONS }} then 0
      when m.median_product_to_cart is null or r.product_to_cart_rate is null then 0
      when r.product_to_cart_rate < (1 - {{ DROP_FRAC }}) * m.median_product_to_cart then 1
      else 0
    end as f_leak_product_to_cart,

    case
      when sessions < {{ MIN_SESSIONS }} then 0
      when m.median_cart_to_purchase is null or r.cart_to_purchase_rate is null then 0
      when r.cart_to_purchase_rate < (1 - {{ DROP_FRAC }}) * m.median_cart_to_purchase then 1
      else 0
    end as f_leak_cart_to_purchase
  from segment_rollup r
  cross join global_medians m
)

-- 7) Return only flagged leaks, worst first
select
  traffic_source,
  state,
  sessions,
  product_sessions,
  atc_sessions,
  atc_given_product_sessions,
  purchases_given_atc_sessions,
  product_to_cart_rate,
  cart_to_purchase_rate,
  median_product_to_cart,
  median_cart_to_purchase,
  product_to_cart_vs_median,
  cart_to_purchase_vs_median,
  f_leak_product_to_cart,
  f_leak_cart_to_purchase
from scored
where (f_leak_product_to_cart = 1 or f_leak_cart_to_purchase = 1)
order by
  coalesce(least(product_to_cart_vs_median, cart_to_purchase_vs_median), 1.0) asc,
  sessions desc
;
