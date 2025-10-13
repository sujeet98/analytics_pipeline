
    
  create or replace table sujeet_data_analytics_workspace.silver_dev.core_commerce_orders
  
  (
    
      global_order_id string COMMENT 'Global BK = source_system:order_id.',
    
      source_system string,
    
      order_id bigint COMMENT 'Source business key.',
    
      user_id bigint COMMENT 'Source user business key.',
    
      order_status string COMMENT 'Source-normalized order status.',
    
      order_date date COMMENT 'DATE(created_at) (or provided by int layer).',
    
      created_at timestamp,
    
      shipped_at timestamp,
    
      delivered_at timestamp,
    
      returned_at timestamp,
    
      num_of_item int,
    
      canonical_updated_at timestamp COMMENT 'Incremental watermark.',
    
      ingest_ts_utc timestamp
    
    
  )

  
  using delta
  
  partitioned by (order_date)
  
  
  
  comment 'Unioned, source-aligned orders across systems. No joins or SCD logic.'
  

  