

with src as (
  select *
  from sujeet_data_analytics_workspace.silver_dev.int_order_items_enriched
  
)

select * from src