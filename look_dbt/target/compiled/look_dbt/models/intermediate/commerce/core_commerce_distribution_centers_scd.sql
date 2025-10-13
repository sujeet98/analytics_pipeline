

-- 1) Versioned rows from per-source snapshots (often static, but SCD2 keeps us safe)
with snap as (
  select
    source_system,
    distribution_center_id,
    name,
    cast(latitude as double)                 as latitude,
    cast(longitude as double)                 as longitude,
    dbt_valid_from,
    dbt_valid_to
  from sujeet_data_analytics_workspace.snapshots.snap_distribution_centers__look

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
  md5(cast(concat(coalesce(cast(global_dc_id as string), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(valid_from as string), '_dbt_utils_surrogate_key_null_')) as string)) as dc_sk,
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