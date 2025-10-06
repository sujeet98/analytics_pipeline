

with source as (
  select * from sujeet_data_analytics_workspace.silver_dev.base_look__orders
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