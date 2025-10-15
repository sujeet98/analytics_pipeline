
    
  create or replace table sujeet_data_analytics_workspace.gold_dev.dim_distribution_center
  
  (
    
      dc_sk string,
    
      global_dc_id string,
    
      source_system string,
    
      distribution_center_id bigint,
    
      name string,
    
      latitude double,
    
      longitude double,
    
      valid_from timestamp,
    
      valid_to timestamp,
    
      is_current boolean,
    
      valid_from_date date
    
    
  )

  
  using delta
  
  
  
  
  
  comment 'Distribution center dimension (SCD2).'
  

  