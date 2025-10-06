
    
  create or replace table sujeet_data_analytics_workspace.gold_dev.users_dim
  
  (
    
      user_id bigint,
    
      first_name string,
    
      last_name string,
    
      email string,
    
      email_domain string,
    
      gender string,
    
      state string,
    
      city string,
    
      country string,
    
      created_at timestamp
    
    
  )

  
  using delta
  
  
  
  
  
  comment 'User dimension (business-conformed users).'
  

  