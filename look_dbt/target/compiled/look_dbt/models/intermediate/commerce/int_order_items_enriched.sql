

with items as ( select * from sujeet_data_analytics_workspace.silver_dev.stg_look__order_items ),
inv   as ( select * from sujeet_data_analytics_workspace.silver_dev.stg_look__inventory_items ),
prod  as ( select * from sujeet_data_analytics_workspace.silver_dev.stg_look__products ),
dc    as ( select * from sujeet_data_analytics_workspace.silver_dev.stg_look__distribution_centers ),

items_joined as (
  select
    i.order_item_id,
    i.order_id,
    i.user_id,
    i.product_id,
    i.inventory_item_id,
    i.status as item_status,
    i.sale_price,
    i.created_at as item_created_at,
    coalesce(inv.product_distribution_center_id, prod.distribution_center_id) as distribution_center_id,
    prod.name as product_name, prod.brand as product_brand, prod.category as product_category, prod.department as product_department, prod.retail_price, prod.sku as product_sku,
    i.src_ingest_ts
  from items i
  left join inv  on i.inventory_item_id = inv.inventory_item_id
  left join prod on i.product_id = prod.product_id
),
items_with_dc as (
  select
    j.*,
    d.name      as distribution_center_name,
    d.latitude  as distribution_center_latitude,
    d.longitude as distribution_center_longitude
  from items_joined j
  left join dc d on j.distribution_center_id = d.distribution_center_id
)

select *
from items_with_dc
