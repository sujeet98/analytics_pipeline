
      
  
  
  
  create or replace view sujeet_data_analytics_workspace.silver_dev.stg_look__distribution_centers
  (
    `distribution_center_id` comment 'PK (from bronze.distribution_centers.id).',
	`name` comment 'Distribution center name.',
	`latitude`,
	`longitude`,
	`distribution_center_geom`,
	`ingest_ts_utc`,
	`source_table`,
	`ingest_date`,
	`run_ts`
  )
  comment 'Cleaned + deduped DCs from bronze_dev.distribution_centers.'
  as (
    with b as (select * from sujeet_data_analytics_workspace.silver_dev.base_look__distribution_centers)
select
  id as distribution_center_id,
  nullif(trim(name), '') as name,
  latitude,
  longitude,
  distribution_center_geom,
  ingest_ts_utc, source_table, ingest_date, run_ts
from b
  )


    