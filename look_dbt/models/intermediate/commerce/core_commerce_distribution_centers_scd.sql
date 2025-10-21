{{ config(
  materialized='table',
  unique_key=['global_dc_id','valid_from'],
  schema='silver_dev',
  tags=['core','commerce','scd2','distribution_centers']
) }}

-- 1) Versioned rows from per-source snapshots (often static, but SCD2 keeps us safe)
with snap as (
  select
    source_system,
    distribution_center_id,
    name_norm as name,
    lat_round                 as latitude,
    lon_round                 as longitude,
    dbt_valid_from,
    dbt_valid_to
  from {{ ref('snap_distribution_centers__look') }}

  -- union all other sources
),

-- 2) Global BK
keyed as (
  select
    concat(source_system, ':', cast(distribution_center_id as string)) as global_dc_id,
    *
  from snap
),

-- 3) Normalize windows + convenience date
normalized as (
  select
    global_dc_id,
    source_system,
    distribution_center_id,
    name,
    latitude,
    longitude,
    dbt_valid_from                                      as valid_from,
    coalesce(dbt_valid_to, cast('9999-12-31 00:00:00' as timestamp))     as valid_to,
    dbt_valid_to is null                                as is_current,
    date(dbt_valid_from)                                as valid_from_date
  from keyed
)

-- 4) Emit one row per version with SCD2 surrogate key
select
  {{ dbt_utils.generate_surrogate_key(['global_dc_id','valid_from']) }} as dc_sk,
  global_dc_id,
  source_system,
  distribution_center_id,
  name,
  latitude,
  longitude,
  valid_from,
  valid_to,
  is_current,
  valid_from_date
from normalized;
