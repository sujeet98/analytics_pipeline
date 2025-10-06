{{ config(materialized='view') }}

with b as (select * from {{ ref('base_look__distribution_centers') }})
select
  id as distribution_center_id,
  {{ clean_string('name') }} as name,
  latitude,
  longitude,
  distribution_center_geom,
  ingest_ts_utc, source_table, ingest_date, run_ts
from b
