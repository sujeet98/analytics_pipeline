
    
  create or replace table sujeet_data_analytics_workspace.silver_dev.core_commerce_order_items
  
  (
    
      global_order_item_id string COMMENT 'Global BK = source_system:order_item_id.',
    
      source_system string,
    
      order_item_id bigint,
    
      order_id bigint COMMENT 'Header FK (source BK).',
    
      user_id bigint COMMENT 'Source user BK (nullable in some sources).',
    
      product_id bigint COMMENT 'Source product BK.',
    
      inventory_item_id bigint COMMENT 'Source inventory item BK (nullable).',
    
      item_status string COMMENT 'Source-normalized line status.',
    
      created_at timestamp,
    
      shipped_at timestamp,
    
      delivered_at timestamp,
    
      returned_at timestamp,
    
      item_date date COMMENT 'DATE(COALESCE(delivered_at, created_at)) provided by int layer.',
    
      created_date date,
    
      sale_price decimal(10, 0) COMMENT 'Non-negative sales price (source value).',
    
      canonical_updated_at timestamp,
    
      ingest_ts_utc timestamp
    
    
  )

  
  using delta
  
  partitioned by (created_date)
  
  
  
  comment 'Unioned, source-aligned order items. No joins or derived metrics.'
  

  