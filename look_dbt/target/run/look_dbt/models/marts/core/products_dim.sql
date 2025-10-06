
    
  create or replace table sujeet_data_analytics_workspace.gold_dev.products_dim
  
  (
    
      product_id bigint,
    
      brand string,
    
      category string,
    
      department string,
    
      sku string,
    
      retail_price double,
    
      distribution_center_id bigint
    
    
  )

  
  using delta
  
  
  
  
  
  
  

  