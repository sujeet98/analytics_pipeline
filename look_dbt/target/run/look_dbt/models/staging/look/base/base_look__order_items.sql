
      
  
  
  
  create or replace view sujeet_data_analytics_workspace.silver_dev.base_look__order_items
  (
    `id`,
	`order_id`,
	`user_id`,
	`product_id`,
	`inventory_item_id`,
	`status`,
	`created_at`,
	`shipped_at`,
	`delivered_at`,
	`returned_at`,
	`sale_price`,
	`ingest_ts_utc`,
	`source_table`,
	`ingest_date`,
	`run_ts`,
	`_rescued_data`,
	`_rn`
  )
  
  as (
    with dedup as (
  with src as (
  select * from `sujeet_data_analytics_workspace`.`bronze_dev`.`order_items`
),
ranked as (
  select
    src.*,
    row_number() over (
      partition by id
      order by ingest_ts_utc desc, to_timestamp(ingest_date) desc
    ) as _rn
  from src
)
select * from ranked where _rn = 1
)
select * from dedup
  )


    