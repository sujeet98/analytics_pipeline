
    
  create or replace table sujeet_data_analytics_workspace.gold_dev.orders
  
  (
    
      order_id bigint,
    
      user_id bigint,
    
      created_at timestamp COMMENT 'UTC timestamp when the order was created.',
    
      status string COMMENT 'Normalized order status.',
    
      item_count bigint,
    
      order_gross_revenue decimal(28, 2) COMMENT 'Non-negative sanity check.'
    
    
  )

  
  using delta
  
  
  
  
  
  comment 'Orders mart with item_count and gross revenue.'
  

  