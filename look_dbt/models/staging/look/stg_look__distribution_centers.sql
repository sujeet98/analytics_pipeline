{{ config(
    materialized = 'view',
    tags = ['staging']
) }}

with source as (
  select * from {{ ref('base_look__distribution_centers') }}
),

renamed as (
  select
    {{ to_bigint_safe('id') }}                             as distribution_center_id,
    cast({{ nullif_blank('name') }} as string)             as name,
    {{ to_double_safe('latitude') }}                       as latitude,
    {{ to_double_safe('longitude') }}                      as longitude,
    cast({{ nullif_blank('distribution_center_geom') }} as string) as distribution_center_geom,

    {{ to_timestamp_safe('ingest_ts_utc') }}               as ingest_ts_utc,
    cast({{ nullif_blank('source_table') }} as string)     as source_table,
    cast({{ nullif_blank('ingest_date') }} as string)      as ingest_date,
    cast({{ nullif_blank('run_ts') }} as string)           as run_ts,
    cast({{ nullif_blank('_rescued_data') }} as string)    as _rescued_data
  from source
)

select * from renamed
