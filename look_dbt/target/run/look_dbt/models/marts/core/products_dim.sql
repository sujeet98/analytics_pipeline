
    
  create or replace table sujeet_data_analytics_workspace.gold_dev.products_dim
  
  (
    
      product_id bigint COMMENT 'Surrogate/product key.',
    
      name string,
    
      brand string,
    
      category string,
    
      department string,
    
      sku string,
    
      retail_price decimal(18, 2),
    
      cost decimal(18, 2),
    
      distribution_center_id bigint COMMENT 'FK to stg_look__distribution_centers.distribution_center_id.'
    
    
  )

  
  using delta
  
  
  
  
  
  comment 'Product dimension at product_id grain.'
  

  