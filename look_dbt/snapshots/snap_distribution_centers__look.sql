{% snapshot snap_distribution_centers__look %}
{{
  config(
    unique_key    = 'distribution_center_id',
    strategy      = 'check',
    check_cols    = ['name_norm','lat_round','lon_round'],
    invalidate_hard_deletes = true
  )
}}

with src as (
  select
    'look' as source_system,
    distribution_center_id,
    -- normalize name and round coords to avoid noise-driven versions
    upper(trim(name))                 as name_norm,
    round(cast(latitude  as double), 6) as lat_round,
    round(cast(longitude as double), 6) as lon_round
  from {{ ref('int_commerce_distribution_centers__look') }}
)

select * from src
{% endsnapshot %}
