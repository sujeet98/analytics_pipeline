
      
  
  
  
  create or replace view sujeet_data_analytics_workspace.silver_dev.base_look__events
  (
    `id`,
	`user_id`,
	`sequence_number`,
	`session_id`,
	`created_at`,
	`ip_address`,
	`city`,
	`state`,
	`postal_code`,
	`browser`,
	`traffic_source`,
	`uri`,
	`event_type`,
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
  select * from `sujeet_data_analytics_workspace`.`bronze_dev`.`events`
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


    