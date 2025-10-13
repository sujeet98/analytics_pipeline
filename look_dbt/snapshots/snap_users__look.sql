{% snapshot snap_users__look %}
{{
  config(
    target_schema = 'snapshots',
    unique_key    = 'user_id',      
    strategy      = 'check',
    check_cols    = [
      'email','first_name','last_name','gender','age',
      'country','state','city','postal_code','traffic_source'
    ],
    invalidate_hard_deletes = true
  )
}}
select
  'look' as source_system,
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
  created_at,            
  user_created_date        
from {{ ref('int_commerce_users__look') }}
{% endsnapshot %}
