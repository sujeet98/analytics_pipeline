
  
  
  create or replace view sujeet_data_analytics_workspace.silver_dev.base_look__orders
  (
    `order_id`,
	`user_id`,
	`status`,
	`gender`,
	`created_at`,
	`returned_at`,
	`shipped_at`,
	`delivered_at`,
	`num_of_item`,
	`ingest_ts_utc`,
	`source_table`,
	`ingest_date`,
	`run_ts`,
	`_rescued_data`
  )
  
  as (
    with src as (
  select * from `sujeet_data_analytics_workspace`.`bronze_dev`.`orders`
),
ranked as (
  select
    *,
    row_number() over (
      partition by order_id
      order by coalesce(ingest_ts_utc, to_timestamp(run_ts)) desc
    ) as rn
  from src
)
select
  order_id,
  user_id,
  status,
  gender,
  created_at,
  returned_at,
  shipped_at,
  delivered_at,
  num_of_item,
  ingest_ts_utc,
  source_table,
  ingest_date,
  run_ts,
  _rescued_data
from ranked
where rn = 1
  )
