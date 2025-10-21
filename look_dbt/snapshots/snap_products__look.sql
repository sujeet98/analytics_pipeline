{% snapshot snap_products__look %}
{{
  config(
    unique_key    = 'product_id',
    strategy      = 'check',
    check_cols    = [
      'product_name_norm','category_norm','brand_norm','department_norm','sku_norm',
      'retail_price_cents','distribution_center_id'
    ],
    invalidate_hard_deletes = true
  )
}}

with src as (
  select
    'look' as source_system,
    product_id,
    -- normalize & stabilize
    product_name                                        as product_name_norm,   -- keep display name but use as check col
    upper(category)                                     as category_norm,
    upper(brand)                                        as brand_norm,
    upper(department)                                   as department_norm,
    upper(sku)                                          as sku_norm,
    cast(round(retail_price * 100, 0) as bigint)        as retail_price_cents,  -- cents to avoid float churn
    distribution_center_id
  from {{ ref('int_commerce_products__look') }}
)

select * from src
{% endsnapshot %}
