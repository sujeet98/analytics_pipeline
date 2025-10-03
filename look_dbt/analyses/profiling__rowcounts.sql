-- Quick sanity profiling: row counts across staged models
select 'stg_look__orders' as model, count(*) from {{ ref('stg_look__orders') }} union all
select 'stg_look__order_items', count(*) from {{ ref('stg_look__order_items') }} union all
select 'stg_look__users', count(*) from {{ ref('stg_look__users') }}
