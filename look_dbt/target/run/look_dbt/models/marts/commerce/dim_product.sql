
    
  create or replace table sujeet_data_analytics_workspace.gold_dev.dim_product
  
  (
    
      product_sk string,
    
      global_product_id string,
    
      source_system string,
    
      product_id bigint,
    
      product_name string,
    
      category string,
    
      brand string,
    
      department string,
    
      sku string,
    
      retail_price double,
    
      distribution_center_id bigint COMMENT 'BK of distribution center (nullable). Used to resolve dc_sk.',
    
      valid_from timestamp,
    
      valid_to timestamp,
    
      is_current boolean,
    
      valid_from_date date
    
    
  )

  
  using delta
  
  
  
  
  
  comment 'Product dimension (SCD2). Carries product attributes and the product\'s distribution_center_id for DC lookups.'
  

  