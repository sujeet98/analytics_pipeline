{{ config(
  materialized='incremental',
  incremental_strategy='insert_overwrite',
  partition_by=['event_date'],
  cluster_by=['customer_sk'],
  tags=['marts','fact','current']
) }}

{% set rewindow_days = 7 %}

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
  from {{ ref('core_commerce_events') }}
  {% if is_incremental() %}
    where event_date >= dateadd(day, -{{ rewindow_days }}, current_date())
  {% endif %}
)

select
  b.event_id,
  dc.customer_sk,
  b.user_id, b.session_id, b.sequence_number,
  b.event_ts, b.event_date,
  b.ip_address, b.city, b.state, b.postal_code,
  b.browser, b.traffic_source, b.uri, b.event_type
from base b
left join {{ ref('dim_customer_current') }} dc
  on b.user_id is not null
 and dc.global_customer_id = concat(b.source_system, ':', cast(b.user_id as string));
