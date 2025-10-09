
    
  create or replace table sujeet_data_analytics_workspace.silver_dev.int_commerce_orders__look
  
  (
    
      order_id bigint COMMENT 'Natural key',
    
      user_id bigint COMMENT 'FK to users.id',
    
      order_status string,
    
      order_date date,
    
      created_at timestamp,
    
      shipped_at timestamp,
    
      delivered_at timestamp,
    
      returned_at timestamp,
    
      num_of_item int,
    
      source_system string,
    
      canonical_updated_at timestamp,
    
      ingest_ts_utc timestamp
    
    
  )

  
  using delta
  
  partitioned by (order_date)
  
  
  
  
  

  