
    
  create or replace table sujeet_data_analytics_workspace.silver_dev.stg_look__products
  
  (
    
      product_id bigint,
    
      unit_cost double,
    
      category string,
    
      product_name string,
    
      brand string,
    
      retail_price double,
    
      department string,
    
      sku string,
    
      distribution_center_id bigint COMMENT 'FK to distribution_centers.distribution_center_id',
    
      ingest_ts_utc timestamp,
    
      _ingest_date string
    
    
  )

  
  using delta
  
  partitioned by (_ingest_date)
  
  
  
  comment 'Staging (deduped) products; latest attributes per product_id.'
  

  