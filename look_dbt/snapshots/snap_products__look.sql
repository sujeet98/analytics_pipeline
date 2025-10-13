{% snapshot snap_products__look %}
{{
  config(
    target_schema = 'snapshots',
    unique_key    = 'product_id',
    strategy      = 'check',
    check_cols    = [
      'product_name','category','brand','department','sku',
      'retail_price','distribution_center_id'
    ],
    invalidate_hard_deletes = true
  )
}}
select
  'look' as source_system,
  product_id,
  product_name,
  category,
  brand,
  department,
  sku,
  cast(retail_price as numeric)       as retail_price,
  distribution_center_id
from {{ ref('int_commerce_products__look') }}
{% endsnapshot %}
