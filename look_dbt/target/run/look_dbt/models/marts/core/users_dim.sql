
    
  create or replace table sujeet_data_analytics_workspace.gold_dev.users_dim
  
  (
    
      user_id bigint,
    
      email string,
    
      first_name string,
    
      last_name string,
    
      age bigint,
    
      gender string,
    
      city string,
    
      state string,
    
      country string,
    
      traffic_source string,
    
      created_at timestamp,
    
      src_ingest_ts timestamp
    
    
  )

  
  using delta
  
  
  
  
  
  
  

  