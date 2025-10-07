
    
  create or replace table sujeet_data_analytics_workspace.gold_dev.distribution_centers_dim
  
  (
    
      distribution_center_id bigint,
    
      distribution_center_name string,
    
      distribution_center_latitude double,
    
      distribution_center_longitude double,
    
      distribution_center_geom string,
    
      src_ingest_ts timestamp
    
    
  )

  
  using delta
  
  
  
  
  
  
  

  