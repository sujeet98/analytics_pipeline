
      
  
  
  
  create or replace view sujeet_data_analytics_workspace.silver_dev.stg_look__users
  (
    `user_id` comment 'PK (from bronze.users.id).',
	`first_name`,
	`last_name`,
	`email` comment 'Raw email string (hashing can be added downstream if needed).',
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
	`run_ts`
  )
  comment 'Cleaned + deduped users from bronze_dev.users.'
  as (
    with b as (select * from sujeet_data_analytics_workspace.silver_dev.base_look__users)
select
  id as user_id,
  nullif(trim(first_name), '') as first_name,
  nullif(trim(last_name), '')  as last_name,
  nullif(trim(email), '')      as email,
  age,
  nullif(trim(gender), '')  as gender,
  nullif(trim(state), '')   as state,
  nullif(trim(street_address), '') as street_address,
  nullif(trim(postal_code), '')    as postal_code,
  nullif(trim(city), '')           as city,
  nullif(trim(country), '')        as country,
  latitude,
  longitude,
  nullif(trim(traffic_source), '') as traffic_source,
  cast(created_at as timestamp) as created_at,
  user_geom,
  ingest_ts_utc, source_table, ingest_date, run_ts
from b
  )


    