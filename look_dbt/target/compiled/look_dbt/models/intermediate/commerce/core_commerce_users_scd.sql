

-- 1) Bring in versioned rows from per-source snapshots.
with snap as (

  -- The Look snapshot (from /snapshots/customers__thelook.sql)
  select
    'look'                          as source_system,
    user_id,
    email,
    first_name,
    last_name,
    gender,
    age,
    country,
    state,
    city,
    postal_code,
    traffic_source,
    dbt_valid_from,
    dbt_valid_to
  from sujeet_data_analytics_workspace.snapshots.snap_users__look

  -- Add more sources as they come online
),

-- 2) Create a global BK that is unique across sources.
keyed as (
  select
    concat(source_system, ':', cast(user_id as string)) as global_customer_id,
    *
  from snap
),

-- 3) Normalize valid windows and compute convenience dates.
normalized as (
  select
    global_customer_id,
    source_system,
    user_id,
    email,
    first_name,
    last_name,
    gender,
    age,
    country,
    state,
    city,
    postal_code,
    traffic_source,

    dbt_valid_from                                             as valid_from,
    coalesce(dbt_valid_to, timestamp('9999-12-31'))            as valid_to,
    dbt_valid_to is null                                       as is_current,
    date(dbt_valid_from)                                       as valid_from_date
  from keyed
)

-- 4) Emit one row per version with a stable SCD2 SK.
select
  md5(cast(concat(coalesce(cast(global_customer_id as string), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(valid_from as string), '_dbt_utils_surrogate_key_null_')) as string)) as customer_sk,  -- SCD2 SK
  global_customer_id,                         -- global BK (keep)
  source_system,
  user_id,                                    -- source BK (keep for audit & external joins)
  email,
  first_name,
  last_name,
  gender,
  age,
  country,
  state,
  city,
  postal_code,
  traffic_source,
  valid_from,
  valid_to,
  is_current,
  valid_from_date
from normalized
;