
    
  create or replace table sujeet_data_analytics_workspace.gold_dev.fact_inventory_items_current
  
  (
    
      inventory_item_id bigint,
    
      product_sk string,
    
      dc_sk string,
    
      created_at timestamp,
    
      created_date date,
    
      sold_at timestamp,
    
      unit_cost double,
    
      retail_price double
    
    
  )

  
  using delta
  
  partitioned by (created_date)
  
  
  
  comment 'Inventory units (CURRENT). Grain: 1 row per inventory_item_id. product_sk and dc_sk resolved to current dims.
'
  

  