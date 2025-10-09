
    
  create or replace table sujeet_data_analytics_workspace.silver_dev.int_commerce_users
  
  (
    
      user_id bigint,
    
      first_name string,
    
      last_name string,
    
      email string COMMENT 'Email may be null or duplicate in sample data; no test by design.',
    
      age int,
    
      gender string COMMENT 'Normalized to m/f (nullable).',
    
      state string,
    
      street_address string,
    
      postal_code string,
    
      city string,
    
      country string,
    
      latitude double,
    
      longitude double,
    
      traffic_source string COMMENT 'Normalized marketing source.',
    
      created_at timestamp,
    
      user_created_date date,
    
      source_system string,
    
      canonical_updated_at timestamp,
    
      ingest_ts_utc timestamp
    
    
  )

  
  using delta
  
  partitioned by (user_created_date)
  
  
  
  comment 'Conformed users (one row per user).'
  

  