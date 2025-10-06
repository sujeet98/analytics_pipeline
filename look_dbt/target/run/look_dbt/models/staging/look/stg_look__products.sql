
  
  
  create or replace view sujeet_data_analytics_workspace.silver_dev.stg_look__products
  (
    `product_id`,
	`cost`,
	`category`,
	`name`,
	`brand`,
	`retail_price`,
	`department`,
	`sku`,
	`distribution_center_id`,
	`ingest_ts_utc`,
	`source_table`,
	`ingest_date`,
	`run_ts`,
	`_rescued_data`
  )
  comment 'Staging view of products (clean naming/types).'
  as (
    with source as (
  select * from sujeet_data_analytics_workspace.silver_dev.base_look__products
),

renamed as (
  select
    /* Safe cast to bigint with NULL on failure */
    try_cast(id AS bigint)                             as product_id,
    /* Safe cast to double with NULL on failure */
    try_cast(cost AS double)                           as cost,
    cast(/* Returns NULL if a value is '' or '-' (we saw '-' in sample data), else the value */
    nullif(nullif(category, ''), '-') as string)         as category,
    cast(/* Returns NULL if a value is '' or '-' (we saw '-' in sample data), else the value */
    nullif(nullif(name, ''), '-') as string)             as name,
    cast(/* Returns NULL if a value is '' or '-' (we saw '-' in sample data), else the value */
    nullif(nullif(brand, ''), '-') as string)            as brand,
    /* Safe cast to double with NULL on failure */
    try_cast(retail_price AS double)                   as retail_price,
    cast(/* Returns NULL if a value is '' or '-' (we saw '-' in sample data), else the value */
    nullif(nullif(department, ''), '-') as string)       as department,
    cast(/* Returns NULL if a value is '' or '-' (we saw '-' in sample data), else the value */
    nullif(nullif(sku, ''), '-') as string)              as sku,
    /* Safe cast to bigint with NULL on failure */
    try_cast(distribution_center_id AS bigint)         as distribution_center_id,

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
