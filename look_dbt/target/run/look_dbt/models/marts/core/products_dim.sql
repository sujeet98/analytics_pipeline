
    
  create or replace table sujeet_data_analytics_workspace.gold_dev.products_dim
  
  (
    
      product_id bigint,
    
      product_name string,
    
      product_brand string,
    
      product_category string,
    
      product_department string,
    
      product_sku string,
    
      retail_price decimal(18, 2),
    
      cost decimal(18, 2),
    
      distribution_center_id bigint,
    
      src_ingest_ts timestamp
    
    
  )

  
  using delta
  
  
  
  
  
  
  

  