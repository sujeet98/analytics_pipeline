
    
  create or replace table sujeet_data_analytics_workspace.silver_dev.stg_look__events
  
  (
    
      event_id bigint,
    
      user_id bigint COMMENT 'FK to users.id; may be null for anonymous traffic.',
    
      sequence_number int,
    
      session_id string,
    
      event_ts timestamp,
    
      event_date date,
    
      ip_address string,
    
      city string,
    
      state string,
    
      postal_code string,
    
      browser string,
    
      traffic_source string,
    
      uri string,
    
      event_type string,
    
      ingest_ts_utc timestamp
    
    
  )

  
  using delta
  
  partitioned by (event_date)
  
  
  
  comment 'Staging (deduped) web/app events; normalized event_type, traffic_source, browser.'
  

  