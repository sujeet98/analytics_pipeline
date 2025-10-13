
    
  create or replace table sujeet_data_analytics_workspace.silver_dev.stg_look__users
  
  (
    
      id bigint,
    
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
    
      ingest_ts_utc timestamp,
    
      _ingest_date string
    
    
  )

  
  using delta
  
  
  
  
  
  comment 'Staging (deduped) users with normalized gender/traffic_source.'
  

  