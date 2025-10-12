
    
  create or replace table sujeet_data_analytics_workspace.silver_dev.stg_look__distribution_centers
  
  (
    
      distribution_center_id bigint,
    
      name string,
    
      latitude double,
    
      longitude double,
    
      ingest_ts_utc timestamp,
    
      _ingest_date string
    
    
  )

  
  using delta
  
  partitioned by (_ingest_date)
  
  
  
  comment 'Staging (deduped) DCs.'
  

  