
    
  create or replace table sujeet_data_analytics_workspace.gold_dev.fact_inventory_items_asof
  
  (
    
      inventory_item_id bigint,
    
      product_sk string,
    
      dc_sk string COMMENT 'Nullable if no DC.',
    
      created_at timestamp,
    
      created_date date,
    
      sold_at timestamp,
    
      unit_cost double,
    
      retail_price double
    
    
  )

  
  using delta
  
  partitioned by (created_date)
  
  
  
  comment 'Inventory units (AS-OF). Grain: 1 row per inventory_item_id. product_sk (as-of created_at) and dc_sk (via product) with earliest-version fallback.
'
  

  