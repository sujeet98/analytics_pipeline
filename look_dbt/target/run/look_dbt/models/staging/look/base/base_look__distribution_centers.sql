
  
  
  create or replace view sujeet_data_analytics_workspace.silver_dev.base_look__distribution_centers
  (
    `id`,
	`name`,
	`latitude`,
	`longitude`,
	`distribution_center_geom`,
	`ingest_ts_utc`,
	`source_table`,
	`ingest_date`,
	`run_ts`,
	`_rescued_data`
  )
  
  as (
    with src as (
  select * from `sujeet_data_analytics_workspace`.`bronze_dev`.`distribution_centers`
),
ranked as (
  select
    *,
    row_number() over (
      partition by id
      order by coalesce(ingest_ts_utc, to_timestamp(run_ts)) desc
    ) as rn
  from src
)
select
  id,
  name,
  latitude,
  longitude,
  distribution_center_geom,
  ingest_ts_utc,
  source_table,
  ingest_date,
  run_ts,
  _rescued_data
from ranked
where rn = 1
  )
