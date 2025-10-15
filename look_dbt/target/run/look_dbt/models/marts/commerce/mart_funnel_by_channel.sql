
    
  create or replace table sujeet_data_analytics_workspace.gold_dev.mart_funnel_by_channel
  
  (
    
      session_date date,
    
      traffic_source string,
    
      sessions bigint,
    
      step_home_rate decimal(6, 5),
    
      step_department_rate decimal(6, 5),
    
      step_product_rate decimal(6, 5),
    
      step_cart_rate decimal(6, 5),
    
      step_purchase_rate decimal(6, 5),
    
      step_cancel_rate decimal(6, 5),
    
      atc_from_product_rate decimal(38, 16),
    
      purchase_click_from_atc_rate decimal(38, 16),
    
      conversion_rate decimal(6, 5),
    
      purchase_actual_from_atc_rate decimal(38, 16),
    
      gmv decimal(38, 0),
    
      aov decimal(38, 6)
    
    
  )

  
  using delta
  
  partitioned by (session_date)
  
  
  
  
  

  