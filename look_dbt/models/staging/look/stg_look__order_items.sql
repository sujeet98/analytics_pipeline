{{ config(
    materialized = 'view',
    tags = ['staging']
) }}

with source as (
    select * from {{ ref('base_look__order_items') }}
),

renamed as (
    select
        {{ to_bigint_safe('id') }}                           as id,                    -- line item PK
        {{ to_bigint_safe('order_id') }}                     as order_id,
        {{ to_bigint_safe('user_id') }}                      as user_id,
        {{ to_bigint_safe('product_id') }}                   as product_id,
        {{ to_bigint_safe('inventory_item_id') }}            as inventory_item_id,

        cast({{ nullif_blank('status') }} as string)         as status,
        {{ to_timestamp_safe('created_at') }}                as created_at,
        {{ to_timestamp_safe('shipped_at') }}                as shipped_at,
        {{ to_timestamp_safe('delivered_at') }}              as delivered_at,
        {{ to_timestamp_safe('returned_at') }}               as returned_at,

        {{ to_double_safe('sale_price') }}                   as sale_price,

        {{ to_timestamp_safe('ingest_ts_utc') }}             as ingest_ts_utc,
        cast({{ nullif_blank('source_table') }} as string)   as source_table,
        cast({{ nullif_blank('ingest_date') }} as string)    as ingest_date,
        cast({{ nullif_blank('run_ts') }} as string)         as run_ts,
        cast({{ nullif_blank('_rescued_data') }} as string)  as _rescued_data

    from source
)

select * from renamed
