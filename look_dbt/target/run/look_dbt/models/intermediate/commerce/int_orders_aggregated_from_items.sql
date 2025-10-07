
      
  
  
  
  create or replace view sujeet_data_analytics_workspace.silver_dev.int_orders_aggregated_from_items
  (
    `order_id`,
	`user_id` comment 'Canonical user_id from orders staging.',
	`order_first_item_at`,
	`item_count` comment 'Must be non-negative.',
	`items_gross_revenue` comment 'Sum of sale_price; non-negative.'
  )
  comment 'One row per order with items aggregation.'
  as (
    with items as (
  select * from sujeet_data_analytics_workspace.silver_dev.int_order_items_enriched
),
orders as (
  -- canonical one row per order with the correct user_id
  select order_id, user_id, created_at
  from sujeet_data_analytics_workspace.silver_dev.stg_look__orders
),
agg as (
  select
      i.order_id,
      -- choose canonical user_id from orders to ensure uniqueness
      o.user_id as user_id,
      min(i.item_created_at)           as order_first_item_at,
      count(*)                         as item_count,
      sum(coalesce(i.sale_price, 0.0)) as items_gross_revenue
  from items i
  left join orders o
    on i.order_id = o.order_id
  group by i.order_id, o.user_id
)

select * from agg
  )


    