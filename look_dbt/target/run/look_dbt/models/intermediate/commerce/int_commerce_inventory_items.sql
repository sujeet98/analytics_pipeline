
    
  create or replace table sujeet_data_analytics_workspace.silver_dev.int_commerce_inventory_items
  
  (
    
      inventory_item_id bigint,
    
      product_id bigint COMMENT 'FK to products.',
    
      created_at timestamp,
    
      sold_at timestamp,
    
      created_date date,
    
      unit_cost double COMMENT 'Non-negative unit cost.',
    
      retail_price double COMMENT 'Non-negative retail price.',
    
      product_category string,
    
      product_name string,
    
      product_brand string,
    
      product_department string,
    
      product_sku string,
    
      distribution_center_id bigint COMMENT 'FK to distribution_centers (nullable).',
    
      source_system string,
    
      canonical_updated_at timestamp,
    
      ingest_ts_utc timestamp
    
    
  )

  
  using delta
  
  partitioned by (created_date)
  
  
  
  comment 'Conformed inventory units (one row per inventory_item_id).'
  

  