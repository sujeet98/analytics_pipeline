
    
  create or replace table sujeet_data_analytics_workspace.silver_dev.core_commerce_distribution_centers_scd
  
  (
    
      dc_sk string,
    
      global_dc_id string COMMENT 'Global BK = source_system:distribution_center_id.',
    
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
  
  
  
  
  
  comment 'Conformed SCD2 distribution centers (only if DCs can change).'
  

  