
    
  create or replace table sujeet_data_analytics_workspace.silver_dev.core_commerce_inventory_items
  
  (
    
      global_inventory_item_id string COMMENT 'Global BK = source_system:inventory_item_id.',
    
      source_system string,
    
      inventory_item_id bigint,
    
      product_id bigint COMMENT 'Source product BK.',
    
      distribution_center_id bigint COMMENT 'Nullable source DC BK.',
    
      created_at timestamp,
    
      sold_at timestamp,
    
      created_date date,
    
      unit_cost double COMMENT 'Non-negative unit cost (source value).',
    
      retail_price double COMMENT 'Non-negative retail price (source value).',
    
      product_category string,
    
      product_name string,
    
      product_brand string,
    
      product_department string,
    
      product_sku string,
    
      canonical_updated_at timestamp,
    
      ingest_ts_utc timestamp
    
    
  )

  
  using delta
  
  partitioned by (created_date)
  
  
  
  comment 'Unioned inventory units. No derived metrics.'
  

  