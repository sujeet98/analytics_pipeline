
    
  create or replace table sujeet_data_analytics_workspace.silver_dev.int_commerce_users__look
  
  (
    
      user_id bigint,
    
      first_name string,
    
      last_name string,
    
      email string,
    
      age int,
    
      gender string,
    
      state string,
    
      street_address string,
    
      postal_code string,
    
      city string,
    
      country string,
    
      latitude double,
    
      longitude double,
    
      traffic_source string,
    
      created_at timestamp,
    
      user_created_date date,
    
      source_system string,
    
      canonical_updated_at timestamp,
    
      ingest_ts_utc timestamp
    
    
  )

  
  using delta
  
  
  
  
  
  
  

  