
    
  create or replace table sujeet_data_analytics_workspace.silver_dev.stg_look__inventory_items
  
  (
    
      id bigint,
    
      product_id bigint COMMENT 'FK to products.product_id',
    
      created_at timestamp,
    
      sold_at timestamp,
    
      unit_cost double,
    
      product_category string,
    
      product_name string,
    
      product_brand string,
    
      retail_price double,
    
      product_department string,
    
      product_sku string,
    
      product_distribution_center_id bigint COMMENT 'FK to distribution_centers.distribution_center_id',
    
      ingest_ts_utc timestamp,
    
      created_date date
    
    
  )

  
  using delta
  
  partitioned by (created_date)
  
  
  
  comment 'Staging (deduped) inventory units with product attributes; latest per id.'
  

  