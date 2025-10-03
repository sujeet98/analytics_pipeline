{{ config(materialized='view') }}

-- Purpose: Stage distribution centers (locations). No joins.
-- Grain: 1 row per distribution_center_id

with source as (
  select * from {{ source('look','distribution_centers') }}
), renamed as (
  select
    cast(id as bigint) as distribution_center_id,
    name, latitude, longitude, distribution_center_geom,
    ingest_ts_utc,
    row_number() over (partition by id order by ingest_ts_utc desc) as _rn
  from source
)
select * from renamed where _rn = 1
