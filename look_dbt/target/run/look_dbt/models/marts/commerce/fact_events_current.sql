
    
  create or replace table sujeet_data_analytics_workspace.gold_dev.fact_events_current
  
  (
    
      event_id bigint,
    
      customer_sk string,
    
      user_id bigint COMMENT 'Nullable for anonymous traffic.',
    
      session_id string,
    
      sequence_number int,
    
      event_ts timestamp,
    
      event_date date,
    
      ip_address string,
    
      city string,
    
      state string,
    
      postal_code string,
    
      browser string,
    
      traffic_source string,
    
      uri string,
    
      event_type string
    
    
  )

  
  using delta
  
  partitioned by (event_date)
  
  
  
  comment 'Event stream (CURRENT). Grain: 1 row per event_id. customer_sk resolved to current dim (nullable for anonymous). Incremental insert_overwrite by event_date.
'
  

  