
      
  
  
  
  create or replace view sujeet_data_analytics_workspace.silver_dev.base_look__users
  (
    `id`,
	`first_name`,
	`last_name`,
	`email`,
	`age`,
	`gender`,
	`state`,
	`street_address`,
	`postal_code`,
	`city`,
	`country`,
	`latitude`,
	`longitude`,
	`traffic_source`,
	`created_at`,
	`user_geom`,
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
  select * from `sujeet_data_analytics_workspace`.`bronze_dev`.`users`
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


    