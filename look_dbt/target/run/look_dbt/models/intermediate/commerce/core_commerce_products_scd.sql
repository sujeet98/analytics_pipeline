
    
  create or replace table sujeet_data_analytics_workspace.silver_dev.core_commerce_products_scd
  
  (
    
      product_sk string,
    
      global_product_id string COMMENT 'Global BK = source_system:product_id.',
    
      source_system string,
    
      product_id bigint,
    
      product_name string,
    
      category string,
    
      brand string,
    
      department string,
    
      sku string,
    
      retail_price double,
    
      distribution_center_id bigint COMMENT 'FK to distribution_centers.distribution_center_id',
    
      valid_from timestamp,
    
      valid_to timestamp,
    
      is_current boolean,
    
      valid_from_date date
    
    
  )

  
  using delta
  
  
  
  
  
  comment 'Conformed SCD2 product dimension built by unioning per-source snapshots.'
  

  