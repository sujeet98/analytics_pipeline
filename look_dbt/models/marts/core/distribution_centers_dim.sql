-- Distribution Center dimension.

{{ config(materialized='table') }}

select
  distribution_center_id,
  name       as distribution_center_name,
  latitude   as distribution_center_latitude,
  longitude  as distribution_center_longitude,
  distribution_center_geom,
  src_ingest_ts
from {{ ref('stg_look__distribution_centers') }};
