{{ config(
    materialized = 'view',
    tags = ['bi','view','growth']
) }}

select
  traffic_source,
  state,
  sessions,
  product_sessions,
  atc_sessions,
  atc_given_product_sessions,
  purchases_given_atc_sessions,
  product_to_cart_rate,
  cart_to_purchase_rate,
  product_to_cart_vs_median,
  cart_to_purchase_vs_median,
  greatest(
    coalesce(1.0 - product_to_cart_vs_median, 0.0),
    coalesce(1.0 - cart_to_purchase_vs_median, 0.0)
  ) as worst_gap,
  case
    when product_to_cart_vs_median is null and cart_to_purchase_vs_median is null then 'no data'
    when least(coalesce(product_to_cart_vs_median, 1.0), coalesce(cart_to_purchase_vs_median, 1.0)) <= 0.50 then 'severe'
    when least(coalesce(product_to_cart_vs_median, 1.0), coalesce(cart_to_purchase_vs_median, 1.0)) <= 0.75 then 'moderate'
    else 'mild'
  end as severity_band
from {{ ref('mart_funnel_leak') }};
