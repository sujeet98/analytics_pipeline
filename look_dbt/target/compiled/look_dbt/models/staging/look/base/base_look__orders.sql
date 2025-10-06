
with dedup as (
  with src as (
  select * from `sujeet_data_analytics_workspace`.`bronze_dev`.`orders`
),
ranked as (
  select
    src.*,
    row_number() over (
      partition by order_id
      order by ingest_ts_utc desc, to_timestamp(ingest_date) desc
    ) as _rn
  from src
)
select * from ranked where _rn = 1
)
select * from dedup