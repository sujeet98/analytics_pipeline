{% snapshot snap_distribution_centers__look %}
{{
  config(
    target_schema = 'snapshots',
    unique_key    = 'distribution_center_id',
    strategy      = 'check',
    check_cols    = ['name','latitude','longitude'],
    invalidate_hard_deletes = true
  )
}}
select
  'look' as source_system,
  distribution_center_id,
  name,
  latitude,
  longitude
from {{ ref('int_commerce_distribution_centers__look') }}
{% endsnapshot %}
