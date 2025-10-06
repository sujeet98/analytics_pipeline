{{ config(materialized='view', tags=['staging']) }}

with source as (
    select * from {{ source('look','events') }}
),

renamed as (
    select
        {{ to_bigint_safe('id') }}                          as id,
        {{ to_bigint_safe('user_id') }}                     as user_id,
        {{ to_bigint_safe('sequence_number') }}             as sequence_number,
        cast({{ nullif_blank('session_id') }} as string)    as session_id,

        {{ to_timestamp_safe('created_at') }}               as created_at,

        cast({{ nullif_blank('ip_address') }} as string)    as ip_address,
        cast({{ nullif_blank('city') }} as string)          as city,
        cast({{ nullif_blank('state') }} as string)         as state,
        cast({{ nullif_blank('postal_code') }} as string)   as postal_code,

        cast({{ nullif_blank('browser') }} as string)       as browser,
        cast({{ nullif_blank('traffic_source') }} as string)as traffic_source,
        cast({{ nullif_blank('uri') }} as string)           as uri,

        -- normalize event types to a controlled set
        case lower(trim(event_type))
            when 'pageview' then 'pageview'
            when 'click' then 'click'
            when 'purchase' then 'purchase'
            when 'cancel' then 'cancel'
            when 'add_to_cart' then 'add_to_cart'
            when 'remove_from_cart' then 'remove_from_cart'
            when 'checkout' then 'checkout'
            when 'login' then 'login'
            when 'signup' then 'signup'
            else 'unknown'
        end                                                 as event_type,

        {{ to_timestamp_safe('ingest_ts_utc') }}            as ingest_ts_utc,
        cast({{ nullif_blank('source_table') }} as string)  as source_table,
        cast({{ nullif_blank('ingest_date') }} as string)   as ingest_date,
        cast({{ nullif_blank('run_ts') }} as string)        as run_ts,
        cast({{ nullif_blank('_rescued_data') }} as string) as _rescued_data

    from source
)

select * from renamed
