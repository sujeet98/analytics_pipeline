
  
  
  create or replace view sujeet_data_analytics_workspace.silver_dev.stg_orders
  (
    `order_id`,
	`user_id`,
	`status`,
	`gender`,
	`normalized_status`,
	`created_at`,
	`returned_at`,
	`shipped_at`,
	`delivered_at`,
	`num_of_item`,
	`ingest_ts_utc`,
	`source_table`,
	`ingest_date`,
	`run_ts`,
	`_rescued_data`
  )
  comment 'Staging view of orders: renamed, typed, de-nullified, no joins.'
  as (
    with source as (
    select * from sujeet_data_analytics_workspace.silver_dev.base_look__orders
),

renamed as (
    select
        -- identifiers
        /* Safe cast to bigint with NULL on failure */
    try_cast(order_id AS bigint)                      as order_id,
        /* Safe cast to bigint with NULL on failure */
    try_cast(user_id AS bigint)                       as user_id,

        -- strings/enums
        cast(/* Returns NULL if a value is '' or '-' (we saw '-' in sample data), else the value */
    nullif(nullif(status, ''), '-') as string)          as status,
        cast(/* Returns NULL if a value is '' or '-' (we saw '-' in sample data), else the value */
    nullif(nullif(gender, ''), '-') as string)          as gender,
        lower(
          case
            when /* Lowercase + trim for consistent string comparisons/joins downstream */
    lower(trim(status)) in ('created','processing','shipped','delivered','returned','cancelled','complete')
              then /* Lowercase + trim for consistent string comparisons/joins downstream */
    lower(trim(status))
            else 'unknown'
          end
        )                                                     as normalized_status,

        -- timestamps
        /* Try-cast to timestamp; if it fails (bad format), returns NULL rather than error */
    try_cast(created_at AS timestamp)                 as created_at,
        /* Try-cast to timestamp; if it fails (bad format), returns NULL rather than error */
    try_cast(returned_at AS timestamp)                as returned_at,
        /* Try-cast to timestamp; if it fails (bad format), returns NULL rather than error */
    try_cast(shipped_at AS timestamp)                 as shipped_at,
        /* Try-cast to timestamp; if it fails (bad format), returns NULL rather than error */
    try_cast(delivered_at AS timestamp)               as delivered_at,

        -- numbers
        /* Safe cast to bigint with NULL on failure */
    try_cast(num_of_item AS bigint)                   as num_of_item,

        -- lineage/ops
        /* Try-cast to timestamp; if it fails (bad format), returns NULL rather than error */
    try_cast(ingest_ts_utc AS timestamp)              as ingest_ts_utc,
        cast(/* Returns NULL if a value is '' or '-' (we saw '-' in sample data), else the value */
    nullif(nullif(source_table, ''), '-') as string)    as source_table,
        cast(/* Returns NULL if a value is '' or '-' (we saw '-' in sample data), else the value */
    nullif(nullif(ingest_date, ''), '-') as string)     as ingest_date,
        cast(/* Returns NULL if a value is '' or '-' (we saw '-' in sample data), else the value */
    nullif(nullif(run_ts, ''), '-') as string)          as run_ts,

        -- rescued
        cast(/* Returns NULL if a value is '' or '-' (we saw '-' in sample data), else the value */
    nullif(nullif(_rescued_data, ''), '-') as string)   as _rescued_data

    from source
)

select * from renamed
  )
