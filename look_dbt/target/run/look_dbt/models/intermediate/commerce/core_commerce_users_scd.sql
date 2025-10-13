
    
  create or replace table sujeet_data_analytics_workspace.silver_dev.core_commerce_users_scd
  
  (
    
      customer_sk string COMMENT 'Surrogate key per version.',
    
      global_customer_id string COMMENT 'Global BK = source_system:user_id.',
    
      source_system string,
    
      user_id bigint,
    
      email string,
    
      first_name string,
    
      last_name string,
    
      gender string,
    
      age int,
    
      country string,
    
      state string,
    
      city string,
    
      postal_code string,
    
      traffic_source string,
    
      valid_from timestamp,
    
      valid_to timestamp,
    
      is_current boolean,
    
      valid_from_date date
    
    
  )

  
  using delta
  
  partitioned by (valid_from_date)
  
  
  
  comment 'Conformed SCD2 customer dimension built by unioning per-source snapshots.'
  

  