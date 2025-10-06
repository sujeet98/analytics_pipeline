
    
  create or replace table sujeet_data_analytics_workspace.gold_dev.distribution_centers_dim
  
  (
    
      distribution_center_id bigint COMMENT 'PK (from bronze.distribution_centers.id).',
    
      name string COMMENT 'Distribution center name.',
    
      latitude double,
    
      longitude double
    
    
  )

  
  using delta
  
  
  
  
  
  
  

  