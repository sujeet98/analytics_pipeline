{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='id',
    partition_by=['_ingest_date'],
    tags=['staging','look']
) }}

{% set lookback_days = 2 %}

with tgt_max as (
  select
    {% if is_incremental() %} coalesce(max(ingest_ts_utc), timestamp('1900-01-01'))
    {% else %}                 timestamp('1900-01-01') {% endif %} as max_ts
  from {% if is_incremental() %} {{ this }} {% else %} (select 1) _ {% endif %}
),

src as (
  select
    cast(id as bigint)                       as id,
    cast(product_id as bigint)               as product_id,
    cast(created_at as timestamp)            as created_at,
    cast(sold_at as timestamp)               as sold_at,
    cast(cost as double)                     as unit_cost,
    nullif(product_category,'')              as product_category,
    nullif(product_name,'')                  as product_name,
    nullif(product_brand,'')                 as product_brand,
    cast(product_retail_price as double)     as retail_price,
    nullif(product_department,'')            as product_department,
    nullif(product_sku,'')                   as product_sku,
    cast(product_distribution_center_id as bigint) as product_distribution_center_id,
    cast(ingest_ts_utc as timestamp)         as ingest_ts_utc,
    cast(ingest_date as string)              as _ingest_date
  from {{ source('look','inventory_items') }}
  {% if is_incremental() %}
    where ingest_ts_utc >= dateadd(day, -{{ lookback_days }}, (select max_ts from tgt_max))
  {% endif %}
),
dedup as (
  select *
  from (
    select *,
      row_number() over (partition by id order by ingest_ts_utc desc nulls last) as rn
    from src
  ) where rn = 1
)
select
  id,
  product_id,
  created_at,
  sold_at,
  unit_cost,
  product_category,
  product_name,
  product_brand,
  retail_price,
  product_department,
  product_sku,
  product_distribution_center_id,
  ingest_ts_utc,
  _ingest_date
from dedup;
