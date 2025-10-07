
      
  
  
  
  create or replace view sujeet_data_analytics_workspace.silver_dev.stg_look__distribution_centers
  (
    `distribution_center_id`,
	`name`,
	`latitude`,
	`longitude`,
	`distribution_center_geom`,
	`src_ingest_ts`
  )
  comment 'Distribution centers, deduped by distribution_center_id, typed.'
  as (
    -- Staging model: Distribution Centers
-- Dedup by distribution_center_id.

with raw as (
  select
    id as distribution_center_id,
    name, latitude, longitude, distribution_center_geom,
    coalesce(
      ingest_ts_utc,
      to_timestamp(concat(ingest_date, ' ', run_ts), 'yyyy-MM-dd HHmmss')
    ) as src_ingest_ts
  from `sujeet_data_analytics_workspace`.`bronze_dev`.`distribution_centers`
),

ranked as (
  select
    r.*,
    row_number() over (
      partition by r.distribution_center_id
      order by r.src_ingest_ts desc
    ) as rn
  from raw r
)

select
  cast(distribution_center_id as bigint) as distribution_center_id,
  trim(name)                             as name,
  cast(latitude as double)               as latitude,
  cast(longitude as double)              as longitude,
  trim(distribution_center_geom)         as distribution_center_geom,
  src_ingest_ts
from ranked
where rn = 1
  )


    