with orders as (

    select * from {{ source('thelook', 'orders') }}

),

renamed as (

    select
        cast(order_id as string)         as order_id,
        cast(user_id as string)          as user_id,
        cast(created_at as timestamp)    as created_at,
        cast(shipped_at as timestamp)    as shipped_at,
        cast(delivered_at as timestamp)  as delivered_at,
        cast(returned_at as timestamp)   as returned_at,

        -- keep our ingestion/system cols consistent
        cast(ingest_date as string)      as ingest_date,
        cast(run_ts as string)           as run_ts,
        cast(ingest_ts_utc as timestamp) as ingest_ts_utc,

        'orders'                          as source_table
    from orders
)

select * from renamed
