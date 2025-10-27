{{ config(
    materialized = 'table',
    tags = ['mart','leak','growth']
) }}

{%- set LOOKBACK_DAYS          = var('leak_lookback_days', 1000) -%}
{%- set MIN_SESSIONS           = var('leak_min_sessions', 3) -%}
{%- set DROP_FRAC              = var('leak_drop_threshold', 0.25) -%}
{%- set AVG_MARGIN_PER_ORDER   = var('avg_margin_per_order', 50.0) -%}

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

-- 5) Global medians (only segments with sufficient size)
global_medians as (
  select
    percentile_approx(product_to_cart_rate, 0.5)
      filter (where sessions >= {{ MIN_SESSIONS }}) as median_product_to_cart,
    percentile_approx(cart_to_purchase_rate, 0.5)
      filter (where sessions >= {{ MIN_SESSIONS }}) as median_cart_to_purchase
  from segment_rollup
),

-- 6) Traffic-source medians (only segments with sufficient size, partitioned by source)
source_medians as (
  select
    traffic_source,
    percentile_approx(product_to_cart_rate, 0.5)
      filter (where sessions >= {{ MIN_SESSIONS }}) as median_product_to_cart_src,
    percentile_approx(cart_to_purchase_rate, 0.5)
      filter (where sessions >= {{ MIN_SESSIONS }}) as median_cart_to_purchase_src
  from segment_rollup
  group by traffic_source
),

-- 7) Compare each segment to medians & flag leaks
scored as (
  select
    r.*,
    -- global medians
    g.median_product_to_cart,
    g.median_cart_to_purchase,
    -- source medians
    s.median_product_to_cart_src,
    s.median_cart_to_purchase_src,

    -- vs global
    case
      when g.median_product_to_cart is null or r.product_to_cart_rate is null then null
      else r.product_to_cart_rate / g.median_product_to_cart
    end as product_to_cart_vs_global,

    case
      when g.median_cart_to_purchase is null or r.cart_to_purchase_rate is null then null
      else r.cart_to_purchase_rate / g.median_cart_to_purchase
    end as cart_to_purchase_vs_global,

    -- vs source
    case
      when s.median_product_to_cart_src is null or r.product_to_cart_rate is null then null
      else r.product_to_cart_rate / s.median_product_to_cart_src
    end as product_to_cart_vs_source,

    case
      when s.median_cart_to_purchase_src is null or r.cart_to_purchase_rate is null then null
      else r.cart_to_purchase_rate / s.median_cart_to_purchase_src
    end as cart_to_purchase_vs_source,

    -- Leak flags vs global medians
    case
      when r.sessions < {{ MIN_SESSIONS }} then 0
      when g.median_product_to_cart is null or r.product_to_cart_rate is null then 0
      when r.product_to_cart_rate < (1 - {{ DROP_FRAC }}) * g.median_product_to_cart then 1
      else 0
    end as f_leak_product_to_cart_global,

    case
      when r.sessions < {{ MIN_SESSIONS }} then 0
      when g.median_cart_to_purchase is null or r.cart_to_purchase_rate is null then 0
      when r.cart_to_purchase_rate < (1 - {{ DROP_FRAC }}) * g.median_cart_to_purchase then 1
      else 0
    end as f_leak_cart_to_purchase_global
  from segment_rollup r
  cross join global_medians g
  left  join source_medians s
    on s.traffic_source = r.traffic_source
),

-- 8) Opportunity sizing:
--    Estimate incremental *session-level purchases* if the segment were lifted to
--    (a) source medians, (b) global medians. Then convert to contribution margin.
opportunity as (
  select
    scored.*,

    -- current expected purchase rate at session level (via step-rates)
    (coalesce(product_to_cart_rate, 0.0) * coalesce(cart_to_purchase_rate, 0.0)) as current_purchase_rate,

    -- targets (fallback to current if median is null)
    least(
      1.0,
      coalesce(median_product_to_cart_src, product_to_cart_rate)
      * coalesce(median_cart_to_purchase_src, cart_to_purchase_rate)
    ) as target_purchase_rate_source,

    least(
      1.0,
      coalesce(median_product_to_cart, product_to_cart_rate)
      * coalesce(median_cart_to_purchase, cart_to_purchase_rate)
    ) as target_purchase_rate_global,

    -- incremental purchases (sessions × (target - current), floored at 0)
    greatest(
      0.0,
      sessions * (
        least(
          1.0,
          coalesce(median_product_to_cart_src, product_to_cart_rate)
          * coalesce(median_cart_to_purchase_src, cart_to_purchase_rate)
        )
        - (coalesce(product_to_cart_rate, 0.0) * coalesce(cart_to_purchase_rate, 0.0))
      )
    ) as incremental_purchases_source,

    greatest(
      0.0,
      sessions * (
        least(
          1.0,
          coalesce(median_product_to_cart, product_to_cart_rate)
          * coalesce(median_cart_to_purchase, cart_to_purchase_rate)
        )
        - (coalesce(product_to_cart_rate, 0.0) * coalesce(cart_to_purchase_rate, 0.0))
      )
    ) as incremental_purchases_global
  from scored
),

final as (
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

    -- global medians
    median_product_to_cart,
    median_cart_to_purchase,
    product_to_cart_vs_global,
    cart_to_purchase_vs_global,
    f_leak_product_to_cart_global,
    f_leak_cart_to_purchase_global,

    -- source medians
    median_product_to_cart_src,
    median_cart_to_purchase_src,
    product_to_cart_vs_source,
    cart_to_purchase_vs_source,

    -- targets & opportunities
    current_purchase_rate,
    target_purchase_rate_source,
    target_purchase_rate_global,

    incremental_purchases_source,
    incremental_purchases_global,

    -- choose the better of source/global targets for opportunity sizing
    greatest(incremental_purchases_source, incremental_purchases_global) as incremental_purchases_best,

    -- convert to contribution margin using a tunable average per order
    {{ AVG_MARGIN_PER_ORDER }} * greatest(incremental_purchases_source, incremental_purchases_global)
      as incremental_contribution_margin_best
  from opportunity
)

-- Show the worst leaks first (either step under global median), largest segments up top when tied
select *
from final
where
  (f_leak_product_to_cart_global = 1 or f_leak_cart_to_purchase_global = 1)
order by
  coalesce(least(product_to_cart_vs_global, cart_to_purchase_vs_global), 1.0) asc,
  sessions desc
;
