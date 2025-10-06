{{ config(
    alias = 'stg_orders',               -- warehouse name (optional; keeps names short)
    materialized = 'view',              -- enforced by folder config; making it explicit for clarity
    tags = ['staging']
) }}

with source as (
    select * from {{ source('look','orders') }}
),

renamed as (
    select
        /* ---- identifiers (keep as BIGINT) ---- */
        {{ to_bigint_safe('order_id') }}                      as order_id,
        {{ to_bigint_safe('user_id') }}                       as user_id,

        /* ---- enums/strings ---- */
        -- Keep original case for BI readability; we *also* provide normalized_status for robust joins.
        cast({{ nullif_blank('status') }} as string)          as status,
        cast({{ nullif_blank('gender') }} as string)          as gender,
        lower(
          case
            when {{ trim_lower('status') }} in ('created','processing','shipped','delivered','returned','cancelled','complete')
              then {{ trim_lower('status') }}
            else 'unknown'
          end
        )                                                     as normalized_status,  -- e.g. 'cancelled'

        /* ---- timestamps ---- */
        {{ to_timestamp_safe('created_at') }}                 as created_at,
        {{ to_timestamp_safe('returned_at') }}                as returned_at,
        {{ to_timestamp_safe('shipped_at') }}                 as shipped_at,
        {{ to_timestamp_safe('delivered_at') }}               as delivered_at,

        /* ---- numbers ---- */
        {{ to_bigint_safe('num_of_item') }}                   as num_of_item,

        /* ---- lineage/ops columns (kept for audits; rarely used in marts) ---- */
        {{ to_timestamp_safe('ingest_ts_utc') }}              as ingest_ts_utc,
        cast({{ nullif_blank('source_table') }} as string)    as source_table,
        cast({{ nullif_blank('ingest_date') }} as string)     as ingest_date,
        cast({{ nullif_blank('run_ts') }} as string)          as run_ts,

        /* ---- rescued data ---- */
        cast({{ nullif_blank('_rescued_data') }} as string)   as _rescued_data

    from source
)

select * from renamed
