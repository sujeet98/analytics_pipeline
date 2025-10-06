
      
  
  
  
  create or replace view sujeet_data_analytics_workspace.silver_dev.base_look__products
  (
    `id`,
	`cost`,
	`category`,
	`name`,
	`brand`,
	`retail_price`,
	`department`,
	`sku`,
	`distribution_center_id`,
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
  select * from `sujeet_data_analytics_workspace`.`bronze_dev`.`products`
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


    