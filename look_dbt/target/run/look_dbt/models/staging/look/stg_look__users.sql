
  
  
  create or replace view sujeet_data_analytics_workspace.silver_dev.stg_look__users
  (
    `user_id`,
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
	`_rescued_data`
  )
  comment 'Staging view of users (PII in raw form; later we can mask).'
  as (
    with source as (
  select * from sujeet_data_analytics_workspace.silver_dev.base_look__users
),

renamed as (
  select
    /* Safe cast to bigint with NULL on failure */
    try_cast(id AS bigint)                             as user_id,
    cast(/* Returns NULL if a value is '' or '-' (we saw '-' in sample data), else the value */
    nullif(nullif(first_name, ''), '-') as string)       as first_name,
    cast(/* Returns NULL if a value is '' or '-' (we saw '-' in sample data), else the value */
    nullif(nullif(last_name, ''), '-') as string)        as last_name,
    cast(/* Returns NULL if a value is '' or '-' (we saw '-' in sample data), else the value */
    nullif(nullif(email, ''), '-') as string)            as email,
    /* Safe cast to bigint with NULL on failure */
    try_cast(age AS bigint)                            as age,
    cast(/* Returns NULL if a value is '' or '-' (we saw '-' in sample data), else the value */
    nullif(nullif(gender, ''), '-') as string)           as gender,
    cast(/* Returns NULL if a value is '' or '-' (we saw '-' in sample data), else the value */
    nullif(nullif(state, ''), '-') as string)            as state,
    cast(/* Returns NULL if a value is '' or '-' (we saw '-' in sample data), else the value */
    nullif(nullif(street_address, ''), '-') as string)   as street_address,
    cast(/* Returns NULL if a value is '' or '-' (we saw '-' in sample data), else the value */
    nullif(nullif(postal_code, ''), '-') as string)      as postal_code,
    cast(/* Returns NULL if a value is '' or '-' (we saw '-' in sample data), else the value */
    nullif(nullif(city, ''), '-') as string)             as city,
    cast(/* Returns NULL if a value is '' or '-' (we saw '-' in sample data), else the value */
    nullif(nullif(country, ''), '-') as string)          as country,
    /* Safe cast to double with NULL on failure */
    try_cast(latitude AS double)                       as latitude,
    /* Safe cast to double with NULL on failure */
    try_cast(longitude AS double)                      as longitude,
    cast(/* Returns NULL if a value is '' or '-' (we saw '-' in sample data), else the value */
    nullif(nullif(traffic_source, ''), '-') as string)   as traffic_source,
    /* Try-cast to timestamp; if it fails (bad format), returns NULL rather than error */
    try_cast(created_at AS timestamp)                  as created_at,
    cast(/* Returns NULL if a value is '' or '-' (we saw '-' in sample data), else the value */
    nullif(nullif(user_geom, ''), '-') as string)        as user_geom,

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
