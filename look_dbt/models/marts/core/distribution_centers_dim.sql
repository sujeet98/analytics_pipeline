{{ config(materialized='table') }}

-- Purpose: Conformed distribution centers
select
  distribution_center_id,
  name,
  latitude,
  longitude
from {{ ref('stg_look__distribution_centers') }}
