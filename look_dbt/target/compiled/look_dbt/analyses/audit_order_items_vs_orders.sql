-- analyses/audit_order_items_vs_orders.sql
-- Identify order_items rows that don't match an order (useful for investigating the 5 failures)
select
  oi.order_id,
  count(*) as item_rows
from sujeet_data_analytics_workspace.silver_dev.stg_look__order_items oi
left join sujeet_data_analytics_workspace.silver_dev.stg_look__orders o using (order_id)
where o.order_id is null
group by 1
order by item_rows desc