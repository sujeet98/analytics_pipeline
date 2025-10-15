



with base as (
  select
    source_system,
    event_id,
    user_id,
    session_id,
    sequence_number,
    event_ts,
    event_date,
    ip_address, city, state, postal_code,
    browser, traffic_source, uri, event_type
  from sujeet_data_analytics_workspace.silver_dev.core_commerce_events
  
),

cust_first as (
  select global_customer_id, min(valid_from) as first_valid_from
  from sujeet_data_analytics_workspace.gold_dev.dim_customer group by 1
),
cust_earliest as (
  select d.*
  from sujeet_data_analytics_workspace.gold_dev.dim_customer d
  join cust_first f
    on d.global_customer_id = f.global_customer_id
   and d.valid_from = f.first_valid_from
),

cust as (
  select
    b.*,
    case
      when b.user_id is null then null
      else coalesce(dc.customer_sk, dce.customer_sk)
    end as customer_sk
  from base b
  left join sujeet_data_analytics_workspace.gold_dev.dim_customer dc
    on b.user_id is not null
   and dc.global_customer_id = concat(b.source_system, ':', cast(b.user_id as string))
   and b.event_ts >= dc.valid_from
   and b.event_ts <  dc.valid_to
  left join cust_earliest dce
    on b.user_id is not null
   and dce.global_customer_id = concat(b.source_system, ':', cast(b.user_id as string))
   and dc.customer_sk is null
)

select
  event_id,
  customer_sk,
  user_id, session_id, sequence_number,
  event_ts, event_date,
  ip_address, city, state, postal_code,
  browser, traffic_source, uri, event_type
from cust;