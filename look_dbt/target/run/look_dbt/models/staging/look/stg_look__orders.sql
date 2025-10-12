
    
  create or replace table sujeet_data_analytics_workspace.silver_dev.stg_look__orders
  
  (
    
      order_id bigint COMMENT 'Natural key',
    
      user_id bigint COMMENT 'FK to users.id',
    
      order_status string,
    
      user_gender string,
    
      created_at timestamp,
    
      shipped_at timestamp,
    
      delivered_at timestamp,
    
      returned_at timestamp,
    
      num_of_item int,
    
      ingest_ts_utc timestamp,
    
      _ingest_date string
    
    
  )

  
  using delta
  
  partitioned by (_ingest_date)
  
  
  
  comment 'Staging (deduped) orders from \'look\' source; latest by ingest_ts_utc per order_id.'
  

  