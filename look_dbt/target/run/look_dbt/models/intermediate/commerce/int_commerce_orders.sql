
    
  create or replace table sujeet_data_analytics_workspace.silver_dev.int_commerce_orders
  
  (
    
      order_id bigint COMMENT 'Business key for an order.',
    
      user_id bigint COMMENT 'FK to users.',
    
      order_status string COMMENT 'Normalized order status.',
    
      order_date date COMMENT 'DATE(created_at).',
    
      created_at timestamp,
    
      shipped_at timestamp,
    
      delivered_at timestamp,
    
      returned_at timestamp,
    
      num_of_item int,
    
      source_system string,
    
      canonical_updated_at timestamp COMMENT 'Watermark used for incremental fan-in.',
    
      ingest_ts_utc timestamp
    
    
  )

  
  using delta
  
  partitioned by (order_date)
  
  
  
  comment 'Conformed orders (union of sources) with survivorship (newest wins).'
  

  