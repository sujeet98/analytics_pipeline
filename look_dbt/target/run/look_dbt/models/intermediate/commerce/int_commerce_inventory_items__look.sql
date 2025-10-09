
    
  create or replace table sujeet_data_analytics_workspace.silver_dev.int_commerce_inventory_items__look
  
  (
    
      inventory_item_id bigint,
    
      product_id bigint COMMENT 'FK to products.product_id',
    
      created_at timestamp,
    
      sold_at timestamp,
    
      created_date date,
    
      unit_cost double,
    
      retail_price double,
    
      product_category string,
    
      product_name string,
    
      product_brand string,
    
      product_department string,
    
      product_sku string,
    
      distribution_center_id bigint COMMENT 'FK to distribution_centers.distribution_center_id',
    
      source_system string,
    
      canonical_updated_at timestamp,
    
      ingest_ts_utc timestamp
    
    
  )

  
  using delta
  
  partitioned by (created_date)
  
  
  
  
  

  