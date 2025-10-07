
    
  create or replace table sujeet_data_analytics_workspace.gold_dev.events
  
  (
    
      event_id bigint,
    
      user_id bigint COMMENT 'Nullable FK — some events are anonymous.',
    
      event_type string,
    
      created_at timestamp,
    
      browser string,
    
      traffic_source string,
    
      uri string,
    
      city string,
    
      state string,
    
      postal_code string,
    
      ip_address string,
    
      src_ingest_ts timestamp
    
    
  )

  
  using delta
  
  
  
  
  
  
  

  