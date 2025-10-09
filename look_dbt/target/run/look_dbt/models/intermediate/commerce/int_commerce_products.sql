
    
  create or replace table sujeet_data_analytics_workspace.silver_dev.int_commerce_products
  
  (
    
      product_id bigint,
    
      product_name string,
    
      brand string,
    
      category string,
    
      department string,
    
      sku string,
    
      unit_cost double COMMENT 'Non-negative unit cost.',
    
      retail_price double COMMENT 'Non-negative retail price.',
    
      distribution_center_id bigint COMMENT 'FK to distribution_centers (nullable).',
    
      product_snap_date date,
    
      source_system string,
    
      canonical_updated_at timestamp,
    
      ingest_ts_utc timestamp
    
    
  )

  
  using delta
  
  partitioned by (product_snap_date)
  
  
  
  comment 'Conformed products (one row per product).'
  

  