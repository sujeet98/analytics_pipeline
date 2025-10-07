
    
  create or replace table sujeet_data_analytics_workspace.gold_dev.orders
  
  (
    
      order_id bigint,
    
      user_id bigint,
    
      status string,
    
      created_at timestamp,
    
      item_count bigint COMMENT 'Derived from items; must be >= 0.',
    
      order_gross_revenue decimal(28, 2) COMMENT 'Sum of item sale_price; must be >= 0.',
    
      src_ingest_ts timestamp
    
    
  )

  
  using delta
  
  
  
  
  
  comment 'Core order grain with item aggregates.'
  

  