{{ config(
  materialized = 'incremental',
  incremental_strategy = 'merge',
  unique_key = ['session_date','traffic_source'],
  partition_by = ['session_date'],
) }}

-- Reuse your pipeline knobs
{% set lookback_days = var('funnel_lookback_days', 2) %}
{% set attribution_hours = var('funnel_attribution_hours', 24) %}

with
{% if is_incremental() %}
-- Rebuild window for incremental runs (only overwrite the last N days)
last_built as (
  select coalesce(max(session_date), date'1900-01-01') as max_session_date
  from {{ this }}
),
cutoffs as (
  select date_add(max_session_date, -{{ lookback_days }}) as min_session_date_rebuild
  from last_built
),
{% endif %}

/* --------------------------------------------------------------------------
   0) Sessions: from mart_session_funnel (already strict attribution-ready)
      We only need session window + channel + purchase flag + session_date
   -------------------------------------------------------------------------- */
sess as (
  select
    session_id,
    session_date,
    customer_sk,
    session_start_ts,
    session_end_ts,
    traffic_source,
    f_purchase_event
  from {{ ref('mart_session_funnel') }}
  where customer_sk is not null
  {% if is_incremental() %}
    and session_date >= (select min_session_date_rebuild from cutoffs)
  {% endif %}
),

/* --------------------------------------------------------------------------
   1) Qualifying orders: status filtered, timestamp anchor = created_at
   -------------------------------------------------------------------------- */
orders as (
  select
    o.order_id,
    o.customer_sk,
    cast(o.created_at as timestamp) as order_ts
  from {{ ref('fact_orders_current') }} o
  where lower(coalesce(o.order_status,'unknown')) not in
        ('cancelled','void','failed','refunded','returned')
  {% if is_incremental() %}
    and to_date(o.created_at) >= (select min_session_date_rebuild from cutoffs)
  {% endif %}
),

/* --------------------------------------------------------------------------
   2) ORDER → SESSION (strict) — ONE session per order, purchase-intent required
      Tier A: order_ts inside [start,end] → closest to session_end_ts
      Tier B: else latest session ended within {{ attribution_hours }}h BEFORE order_ts
   -------------------------------------------------------------------------- */
-- in-window candidates
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
   and coalesce(s.f_purchase_event,0) = 1
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

-- lookback candidates (ended BEFORE the order, within window)
tier_b as (
  select
    o.order_id,
    s.session_id,
    s.session_end_ts,
    s.session_id as sid
  from orders o
  join sess s
    on s.customer_sk = o.customer_sk
   and coalesce(s.f_purchase_event,0) = 1
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

order_to_session as (
  select
    o.order_id,
    coalesce(a.session_id, b.session_id) as session_id
  from orders o
  left join pick_a a using (order_id)
  left join pick_b b using (order_id)
),

/* --------------------------------------------------------------------------
   3) Push chosen session/channel to each order item
   -------------------------------------------------------------------------- */
items_with_channel as (
  select
    i.order_item_id,
    i.order_id,
    i.product_sk,
    cast(i.sale_price as double)                 as sale_price,
    case when i.returned_at is not null then 1 else 0 end as f_return,
    cast(i.created_at as timestamp)              as item_ts,
    cast(date(i.created_date) as date)           as item_date,
    s.session_date,
    s.traffic_source
  from {{ ref('fact_order_items_current') }} i
  left join order_to_session ots on ots.order_id = i.order_id
  left join sess s               on s.session_id = ots.session_id
  {% if is_incremental() %}
    where coalesce(s.session_date, i.item_date) >= (select min_session_date_rebuild from cutoffs)
  {% endif %}
),

/* --------------------------------------------------------------------------
   4) Cost assignment:
      Prefer SAME-DAY inventory unit_cost (closest sold_at to item_ts),
      else fall back to product average unit_cost.
   -------------------------------------------------------------------------- */
inventory_day as (
  select
    fi.product_sk,
    date(fi.sold_at)             as sold_date,
    fi.sold_at,
    cast(fi.unit_cost as double) as unit_cost
  from {{ ref('fact_inventory_items_current') }} fi
  where fi.sold_at is not null
),

cost_match_same_day as (
  select
    iwc.order_item_id,
    idy.unit_cost,
    row_number() over (
      partition by iwc.order_item_id
      order by abs(unix_timestamp(idy.sold_at) - unix_timestamp(iwc.item_ts))
    ) as rn
  from items_with_channel iwc
  join inventory_day idy
    on idy.product_sk = iwc.product_sk
   and idy.sold_date  = iwc.item_date
),
best_same_day_cost as (
  select order_item_id, unit_cost
  from cost_match_same_day
  where rn = 1
),
product_avg_cost as (
  select
    fi.product_sk,
    avg(cast(fi.unit_cost as double)) as avg_unit_cost
  from {{ ref('fact_inventory_items_current') }} fi
  where fi.unit_cost is not null
  group by 1
),
items_costed as (
  select
    iwc.*,
    coalesce(bsc.unit_cost, pac.avg_unit_cost, 0.0) as unit_cost
  from items_with_channel iwc
  left join best_same_day_cost bsc
    on bsc.order_item_id = iwc.order_item_id
  left join product_avg_cost pac
    on pac.product_sk = iwc.product_sk
),

/* --------------------------------------------------------------------------
   5) Daily channel economics from items (return-adjusted)
   -------------------------------------------------------------------------- */
item_margins as (
  select
    coalesce(iwc.session_date, iwc.item_date)     as session_date,
    coalesce(iwc.traffic_source, 'Unattributed')  as traffic_source,
    count(*)                                      as items,
    sum(iwc.sale_price)                           as gmv_items,
    sum(iwc.sale_price - iwc.unit_cost)           as gross_margin_pre_returns,
    sum(case when iwc.f_return = 1
             then (iwc.sale_price - iwc.unit_cost) else 0 end) as return_margin_hit,
    sum(case when iwc.f_return = 0
             then (iwc.sale_price - iwc.unit_cost) else 0 end) as contribution_margin
  from items_costed iwc
  group by 1,2
),

/* --------------------------------------------------------------------------
   6) Sessions per channel/day (for normalization & sanity checks)
      Use your aligned funnel mart to keep attribution consistent.
   -------------------------------------------------------------------------- */
channel_funnel as (
  select
    m.session_date,
    m.traffic_source,
    m.sessions,
    m.orders_count,
    cast(m.gmv as double)             as gmv_reported,
    cast(m.aov as double)             as aov,
    cast(m.conversion_rate as double) as conversion_rate
  from {{ ref('mart_funnel_by_channel') }} m
  {% if is_incremental() %}
    where m.session_date >= (select min_session_date_rebuild from cutoffs)
  {% endif %}
),

/* --------------------------------------------------------------------------
   7) Final join + “per 1k sessions”
   -------------------------------------------------------------------------- */
final as (
  select
    coalesce(cf.session_date, im.session_date)       as session_date,
    coalesce(cf.traffic_source, im.traffic_source)   as traffic_source,

    -- top-of-funnel context
    coalesce(cf.sessions, 0)                         as sessions,
    coalesce(cf.orders_count, 0)                     as orders_count,
    coalesce(cf.gmv_reported, 0)                     as gmv_reported,

    -- item economics
    coalesce(im.items, 0)                            as items,
    coalesce(im.gmv_items, 0)                        as gmv_items,
    coalesce(im.gross_margin_pre_returns, 0)         as gross_margin_pre_returns,
    coalesce(im.return_margin_hit, 0)                as return_margin_hit,
    coalesce(im.contribution_margin, 0)              as contribution_margin,

    -- normalized profit signal
    case when coalesce(cf.sessions,0) > 0
         then 1000.0 * coalesce(im.contribution_margin,0) / cf.sessions
         else null
    end                                              as contribution_margin_per_1000_sessions,

    -- helpers
    case when coalesce(im.contribution_margin,0) < 0 then 1 else 0 end as f_negative_contribution,
    cf.aov,
    cf.conversion_rate
  from channel_funnel cf
  full outer join item_margins im
    on im.session_date   = cf.session_date
   and im.traffic_source = cf.traffic_source
)

select *
from final
{% if is_incremental() %}
where session_date >= (select min_session_date_rebuild from cutoffs)
{% endif %}
order by session_date, traffic_source;
