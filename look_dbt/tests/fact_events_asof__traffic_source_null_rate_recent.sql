{{ config(severity='warn') }}

-- Fails (warns) if more than 2% of recent events have NULL traffic_source
with recent as (
  select *
  from {{ ref('fact_events_asof') }}
  where event_date >= date_add(current_date(), -30)
),
agg as (
  select
    count(*) as n_rows,
    sum(case when traffic_source is null then 1 else 0 end) as null_rows,
    case when count(*) = 0 then 0.0
         else (100.0 * sum(case when traffic_source is null then 1 else 0 end) / count(*))
    end as null_rate_pct
  from recent
)
select
  n_rows,
  null_rows,
  null_rate_pct,
  'Expect < 2% NULL traffic_source in last 30 days' as failure_reason
from agg
where null_rate_pct >= 2.0
