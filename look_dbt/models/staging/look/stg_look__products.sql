{{ config(
    materialized = 'view',
    tags = ['staging']
) }}

with source as (
  select * from {{ ref('base_look__products') }}
),

renamed as (
  select
    {{ to_bigint_safe('id') }}                             as product_id,
    {{ to_double_safe('cost') }}                           as cost,
    cast({{ nullif_blank('category') }} as string)         as category,
    cast({{ nullif_blank('name') }} as string)             as name,
    cast({{ nullif_blank('brand') }} as string)            as brand,
    {{ to_double_safe('retail_price') }}                   as retail_price,
    cast({{ nullif_blank('department') }} as string)       as department,
    cast({{ nullif_blank('sku') }} as string)              as sku,
    {{ to_bigint_safe('distribution_center_id') }}         as distribution_center_id,

    {{ to_timestamp_safe('ingest_ts_utc') }}               as ingest_ts_utc,
    cast({{ nullif_blank('source_table') }} as string)     as source_table,
    cast({{ nullif_blank('ingest_date') }} as string)      as ingest_date,
    cast({{ nullif_blank('run_ts') }} as string)           as run_ts,
    cast({{ nullif_blank('_rescued_data') }} as string)    as _rescued_data
  from source
)

select * from renamed
