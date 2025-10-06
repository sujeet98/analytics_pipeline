

with source as (
  select * from sujeet_data_analytics_workspace.silver_dev.base_look__inventory_items
),

renamed as (
  select
    /* Safe cast to bigint with NULL on failure */
    try_cast(id AS bigint)                             as inventory_item_id,
    /* Safe cast to bigint with NULL on failure */
    try_cast(product_id AS bigint)                     as product_id,
    /* Try-cast to timestamp; if it fails (bad format), returns NULL rather than error */
    try_cast(created_at AS timestamp)                  as created_at,
    /* Try-cast to timestamp; if it fails (bad format), returns NULL rather than error */
    try_cast(sold_at AS timestamp)                     as sold_at,
    /* Safe cast to double with NULL on failure */
    try_cast(cost AS double)                           as cost,

    cast(/* Returns NULL if a value is '' or '-' (we saw '-' in sample data), else the value */
    nullif(nullif(product_category, ''), '-') as string) as product_category,
    cast(/* Returns NULL if a value is '' or '-' (we saw '-' in sample data), else the value */
    nullif(nullif(product_name, ''), '-') as string)     as product_name,
    cast(/* Returns NULL if a value is '' or '-' (we saw '-' in sample data), else the value */
    nullif(nullif(product_brand, ''), '-') as string)    as product_brand,
    /* Safe cast to double with NULL on failure */
    try_cast(product_retail_price AS double)           as product_retail_price,
    cast(/* Returns NULL if a value is '' or '-' (we saw '-' in sample data), else the value */
    nullif(nullif(product_department, ''), '-') as string) as product_department,
    cast(/* Returns NULL if a value is '' or '-' (we saw '-' in sample data), else the value */
    nullif(nullif(product_sku, ''), '-') as string)      as product_sku,
    /* Safe cast to bigint with NULL on failure */
    try_cast(product_distribution_center_id AS bigint) as product_distribution_center_id,

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