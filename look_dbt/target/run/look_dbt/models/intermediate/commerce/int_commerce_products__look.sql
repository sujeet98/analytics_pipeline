
    
  create or replace table sujeet_data_analytics_workspace.silver_dev.int_commerce_products__look
  
  (
    
      product_id bigint,
    
      product_name string,
    
      brand string,
    
      category string,
    
      department string,
    
      sku string,
    
      unit_cost double,
    
      retail_price double,
    
      distribution_center_id bigint COMMENT 'FK to distribution_centers.distribution_center_id',
    
      product_snap_date date,
    
      source_system string,
    
      canonical_updated_at timestamp,
    
      ingest_ts_utc timestamp
    
    
  )

  
  using delta
  
  
  
  
  
  
  

  