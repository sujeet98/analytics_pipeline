
    
  create or replace table sujeet_data_analytics_workspace.gold_dev.events
  
  (
    
      event_id bigint,
    
      user_id bigint,
    
      session_id string,
    
      sequence_number bigint,
    
      created_at timestamp COMMENT 'UTC timestamp of the event.',
    
      event_type string COMMENT 'Normalized event type.',
    
      city string,
    
      state string,
    
      postal_code string,
    
      browser string,
    
      traffic_source string,
    
      uri string,
    
      ip_address string
    
    
  )

  
  using delta
  
  
  
  
  
  comment 'Event facts, one row per event.'
  

  