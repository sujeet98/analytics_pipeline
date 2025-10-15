
    
  create or replace table sujeet_data_analytics_workspace.gold_dev.dim_customer
  
  (
    
      customer_sk string COMMENT 'SCD2 surrogate key (PK).',
    
      global_customer_id string COMMENT 'Global BK: <source_system>:<user_id>.',
    
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
  
  
  
  
  
  comment 'Customer dimension (SCD2). One row per version; joins from facts use customer_sk (AS-OF) or the dim_customer_current view (CURRENT).'
  

  