{#
  Purpose
  -------
  Distribution center dimension.
#}

{{ config(materialized='table') }}

select
  distribution_center_id,
  name,
  latitude,
  longitude
from {{ ref('stg_look__distribution_centers') }}
;
