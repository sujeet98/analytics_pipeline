
  
  
  create or replace view sujeet_data_analytics_workspace.default_silver_dev.stg_look__distribution_centers
  (
    `distribution_center_id`,
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
  comment 'Staging view of distribution centers; geo kept as strings.'
  as (
    with source as (
  select * from `sujeet_data_analytics_workspace`.`bronze_dev`.`distribution_centers`
),

renamed as (
  select
    /* Safe cast to bigint with NULL on failure */
    try_cast(id AS bigint)                             as distribution_center_id,
    cast(/* Returns NULL if a value is '' or '-' (we saw '-' in sample data), else the value */
    nullif(nullif(name, ''), '-') as string)             as name,
    /* Safe cast to double with NULL on failure */
    try_cast(latitude AS double)                       as latitude,
    /* Safe cast to double with NULL on failure */
    try_cast(longitude AS double)                      as longitude,
    cast(/* Returns NULL if a value is '' or '-' (we saw '-' in sample data), else the value */
    nullif(nullif(distribution_center_geom, ''), '-') as string) as distribution_center_geom,

    /* Try-cast to timestamp; if it fails (bad format), returns NULL rather than error */
    try_cast(ingest_ts_utc AS timestamp)               as ingest_ts_utc,
    cast(/* Returns NULL if a value is '' or '-' (we saw '-' in sample data), else the value */
    nullif(nullif(source_table, ''), '-') as string)     as source_table,
    cast(/* Returns NULL if a value is '' or '-' (we saw '-' in sample data), else the value */
    nullif(nullif(ingest_date, ''), '-') as string)      as ingest_date,
    cast(/* Returns NULL if a value is '' or '-' (we saw '-' in sample data), else the value */
    nullif(nullif(run_ts, ''), '-') as string)           as run_ts,
    cast(/* Returns NULL if a value is '' or '-' (we saw '-' in sample data), else the value */
    nullif(nullif(_rescued_data, ''), '-') as string)    as _rescued_data
  from source
)

select * from renamed
  )
