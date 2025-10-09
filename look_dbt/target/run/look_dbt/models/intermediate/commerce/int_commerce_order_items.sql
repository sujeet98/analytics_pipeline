
    
  create or replace table sujeet_data_analytics_workspace.silver_dev.int_commerce_order_items
  
  (
    
      order_item_id bigint COMMENT 'Business key for a line item.',
    
      order_id bigint COMMENT 'FK to orders.',
    
      user_id bigint COMMENT 'FK to users (nullable if absent upstream).',
    
      product_id bigint COMMENT 'FK to products.',
    
      inventory_item_id bigint COMMENT 'FK to inventory_items (nullable).',
    
      item_status string COMMENT 'Normalized line status.',
    
      created_at timestamp,
    
      shipped_at timestamp,
    
      delivered_at timestamp,
    
      returned_at timestamp,
    
      item_date date COMMENT 'DATE(COALESCE(delivered_at, created_at)).',
    
      sale_price double COMMENT 'Non-negative sales price.',
    
      source_system string,
    
      canonical_updated_at timestamp,
    
      ingest_ts_utc timestamp
    
    
  )

  
  using delta
  
  partitioned by (item_date)
  
  
  
  comment 'Conformed order items (union of sources) with survivorship.'
  

  