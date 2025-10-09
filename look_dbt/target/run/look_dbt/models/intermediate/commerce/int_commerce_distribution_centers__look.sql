
    
  create or replace table sujeet_data_analytics_workspace.silver_dev.int_commerce_distribution_centers__look
  
  (
    
      distribution_center_id bigint,
    
      name string,
    
      latitude double,
    
      longitude double,
    
      _snap_date date,
    
      source_system string,
    
      canonical_updated_at timestamp,
    
      ingest_ts_utc timestamp
    
    
  )

  
  using delta
  
  partitioned by (_snap_date)
  
  
  
  
  

  