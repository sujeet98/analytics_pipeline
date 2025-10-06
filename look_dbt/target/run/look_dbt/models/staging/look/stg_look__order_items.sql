
  
  
  create or replace view sujeet_data_analytics_workspace.silver_dev.stg_look__order_items
  (
    `id`,
	`order_id`,
	`user_id`,
	`product_id`,
	`inventory_item_id`,
	`status`,
	`created_at`,
	`shipped_at`,
	`delivered_at`,
	`returned_at`,
	`sale_price`,
	`ingest_ts_utc`,
	`source_table`,
	`ingest_date`,
	`run_ts`,
	`_rescued_data`
  )
  comment 'Staging view of order line items: clean ids, monetary types, timestamps.'
  as (
    with source as (
    select * from sujeet_data_analytics_workspace.silver_dev.base_look__order_items
),

renamed as (
    select
        /* Safe cast to bigint with NULL on failure */
    try_cast(id AS bigint)                           as id,                    -- line item PK
        /* Safe cast to bigint with NULL on failure */
    try_cast(order_id AS bigint)                     as order_id,
        /* Safe cast to bigint with NULL on failure */
    try_cast(user_id AS bigint)                      as user_id,
        /* Safe cast to bigint with NULL on failure */
    try_cast(product_id AS bigint)                   as product_id,
        /* Safe cast to bigint with NULL on failure */
    try_cast(inventory_item_id AS bigint)            as inventory_item_id,

        cast(/* Returns NULL if a value is '' or '-' (we saw '-' in sample data), else the value */
    nullif(nullif(status, ''), '-') as string)         as status,
        /* Try-cast to timestamp; if it fails (bad format), returns NULL rather than error */
    try_cast(created_at AS timestamp)                as created_at,
        /* Try-cast to timestamp; if it fails (bad format), returns NULL rather than error */
    try_cast(shipped_at AS timestamp)                as shipped_at,
        /* Try-cast to timestamp; if it fails (bad format), returns NULL rather than error */
    try_cast(delivered_at AS timestamp)              as delivered_at,
        /* Try-cast to timestamp; if it fails (bad format), returns NULL rather than error */
    try_cast(returned_at AS timestamp)               as returned_at,

        /* Safe cast to double with NULL on failure */
    try_cast(sale_price AS double)                   as sale_price,

        /* Try-cast to timestamp; if it fails (bad format), returns NULL rather than error */
    try_cast(ingest_ts_utc AS timestamp)             as ingest_ts_utc,
        cast(/* Returns NULL if a value is '' or '-' (we saw '-' in sample data), else the value */
    nullif(nullif(source_table, ''), '-') as string)   as source_table,
        cast(/* Returns NULL if a value is '' or '-' (we saw '-' in sample data), else the value */
    nullif(nullif(ingest_date, ''), '-') as string)    as ingest_date,
        cast(/* Returns NULL if a value is '' or '-' (we saw '-' in sample data), else the value */
    nullif(nullif(run_ts, ''), '-') as string)         as run_ts,
        cast(/* Returns NULL if a value is '' or '-' (we saw '-' in sample data), else the value */
    nullif(nullif(_rescued_data, ''), '-') as string)  as _rescued_data

    from source
)

select * from renamed
  )
