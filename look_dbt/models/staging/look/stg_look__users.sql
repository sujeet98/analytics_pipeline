{{ config(
    materialized = 'view',
    tags = ['staging']
) }}

with source as (
  select * from {{ ref('base_look__users') }}
),

renamed as (
  select
    {{ to_bigint_safe('id') }}                             as user_id,
    cast({{ nullif_blank('first_name') }} as string)       as first_name,
    cast({{ nullif_blank('last_name') }} as string)        as last_name,
    cast({{ nullif_blank('email') }} as string)            as email,
    {{ to_bigint_safe('age') }}                            as age,
    cast({{ nullif_blank('gender') }} as string)           as gender,
    cast({{ nullif_blank('state') }} as string)            as state,
    cast({{ nullif_blank('street_address') }} as string)   as street_address,
    cast({{ nullif_blank('postal_code') }} as string)      as postal_code,
    cast({{ nullif_blank('city') }} as string)             as city,
    cast({{ nullif_blank('country') }} as string)          as country,
    {{ to_double_safe('latitude') }}                       as latitude,
    {{ to_double_safe('longitude') }}                      as longitude,
    cast({{ nullif_blank('traffic_source') }} as string)   as traffic_source,
    {{ to_timestamp_safe('created_at') }}                  as created_at,
    cast({{ nullif_blank('user_geom') }} as string)        as user_geom,

    {{ to_timestamp_safe('ingest_ts_utc') }}               as ingest_ts_utc,
    cast({{ nullif_blank('source_table') }} as string)     as source_table,
    cast({{ nullif_blank('ingest_date') }} as string)      as ingest_date,
    cast({{ nullif_blank('run_ts') }} as string)           as run_ts,
    cast({{ nullif_blank('_rescued_data') }} as string)    as _rescued_data
  from source
)

select * from renamed
