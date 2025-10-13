
    
  create or replace table sujeet_data_analytics_workspace.silver_dev.core_commerce_events
  
  (
    
      global_event_id string COMMENT 'Global BK = source_system:event_id.',
    
      source_system string,
    
      event_id bigint,
    
      user_id bigint COMMENT 'Nullable for anonymous traffic.',
    
      session_id string,
    
      sequence_number int,
    
      event_ts timestamp,
    
      event_date date,
    
      ip_address string,
    
      city string,
    
      state string,
    
      postal_code string,
    
      browser string COMMENT 'Browser family.',
    
      traffic_source string COMMENT 'Marketing source.',
    
      uri string,
    
      event_type string COMMENT 'Normalized event type.',
    
      canonical_updated_at timestamp,
    
      ingest_ts_utc timestamp
    
    
  )

  
  using delta
  
  partitioned by (event_date)
  
  
  
  comment 'Unioned event stream across sources. No SCD logic.'
  

  