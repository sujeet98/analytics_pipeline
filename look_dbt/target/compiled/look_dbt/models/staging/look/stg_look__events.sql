

with source as (
    select * from sujeet_data_analytics_workspace.silver_dev.base_look__events
),

renamed as (
    select
        /* Safe cast to bigint with NULL on failure */
    try_cast(id AS bigint)                          as id,
        /* Safe cast to bigint with NULL on failure */
    try_cast(user_id AS bigint)                     as user_id,
        /* Safe cast to bigint with NULL on failure */
    try_cast(sequence_number AS bigint)             as sequence_number,
        cast(/* Returns NULL if a value is '' or '-' (we saw '-' in sample data), else the value */
    nullif(nullif(session_id, ''), '-') as string)    as session_id,

        /* Try-cast to timestamp; if it fails (bad format), returns NULL rather than error */
    try_cast(created_at AS timestamp)               as created_at,

        cast(/* Returns NULL if a value is '' or '-' (we saw '-' in sample data), else the value */
    nullif(nullif(ip_address, ''), '-') as string)    as ip_address,
        cast(/* Returns NULL if a value is '' or '-' (we saw '-' in sample data), else the value */
    nullif(nullif(city, ''), '-') as string)          as city,
        cast(/* Returns NULL if a value is '' or '-' (we saw '-' in sample data), else the value */
    nullif(nullif(state, ''), '-') as string)         as state,
        cast(/* Returns NULL if a value is '' or '-' (we saw '-' in sample data), else the value */
    nullif(nullif(postal_code, ''), '-') as string)   as postal_code,

        cast(/* Returns NULL if a value is '' or '-' (we saw '-' in sample data), else the value */
    nullif(nullif(browser, ''), '-') as string)       as browser,
        cast(/* Returns NULL if a value is '' or '-' (we saw '-' in sample data), else the value */
    nullif(nullif(traffic_source, ''), '-') as string)as traffic_source,
        cast(/* Returns NULL if a value is '' or '-' (we saw '-' in sample data), else the value */
    nullif(nullif(uri, ''), '-') as string)           as uri,

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

        /* Try-cast to timestamp; if it fails (bad format), returns NULL rather than error */
    try_cast(ingest_ts_utc AS timestamp)            as ingest_ts_utc,
        cast(/* Returns NULL if a value is '' or '-' (we saw '-' in sample data), else the value */
    nullif(nullif(source_table, ''), '-') as string)  as source_table,
        cast(/* Returns NULL if a value is '' or '-' (we saw '-' in sample data), else the value */
    nullif(nullif(ingest_date, ''), '-') as string)   as ingest_date,
        cast(/* Returns NULL if a value is '' or '-' (we saw '-' in sample data), else the value */
    nullif(nullif(run_ts, ''), '-') as string)        as run_ts,
        cast(/* Returns NULL if a value is '' or '-' (we saw '-' in sample data), else the value */
    nullif(nullif(_rescued_data, ''), '-') as string) as _rescued_data

    from source
)

select * from renamed