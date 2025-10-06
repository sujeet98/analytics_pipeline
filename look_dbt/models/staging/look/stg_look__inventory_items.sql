{{ config(materialized='view', tags=['staging']) }}

with source as (
  select * from {{ source('look','inventory_items') }}
),

renamed as (
  select
    {{ to_bigint_safe('id') }}                             as inventory_item_id,
    {{ to_bigint_safe('product_id') }}                     as product_id,
    {{ to_timestamp_safe('created_at') }}                  as created_at,
    {{ to_timestamp_safe('sold_at') }}                     as sold_at,
    {{ to_double_safe('cost') }}                           as cost,

    cast({{ nullif_blank('product_category') }} as string) as product_category,
    cast({{ nullif_blank('product_name') }} as string)     as product_name,
    cast({{ nullif_blank('product_brand') }} as string)    as product_brand,
    {{ to_double_safe('product_retail_price') }}           as product_retail_price,
    cast({{ nullif_blank('product_department') }} as string) as product_department,
    cast({{ nullif_blank('product_sku') }} as string)      as product_sku,
    {{ to_bigint_safe('product_distribution_center_id') }} as product_distribution_center_id,

    {{ to_timestamp_safe('ingest_ts_utc') }}               as ingest_ts_utc,
    cast({{ nullif_blank('source_table') }} as string)     as source_table,
    cast({{ nullif_blank('ingest_date') }} as string)      as ingest_date,
    cast({{ nullif_blank('run_ts') }} as string)           as run_ts,
    cast({{ nullif_blank('_rescued_data') }} as string)    as _rescued_data
  from source
)

select * from renamed
