
    
  create or replace table sujeet_data_analytics_workspace.silver_dev.int_commerce_events
  
  (
    
      event_id bigint,
    
      user_id bigint COMMENT 'FK to users (nullable for anonymous).',
    
      sequence_number int,
    
      session_id string,
    
      event_ts timestamp,
    
      event_date date,
    
      ip_address string,
    
      city string,
    
      state string,
    
      postal_code string,
    
      browser string COMMENT 'Normalized browser family.',
    
      traffic_source string COMMENT 'Normalized marketing source.',
    
      uri string,
    
      event_type string COMMENT 'Normalized event type.',
    
      source_system string,
    
      canonical_updated_at timestamp,
    
      ingest_ts_utc timestamp
    
    
  )

  
  using delta
  
  partitioned by (event_date)
  
  
  
  comment 'Conformed web/app events (insert_overwrite by event_date).'
  

  