
    
  create or replace table sujeet_data_analytics_workspace.gold_dev.order_items
  
  (
    
      order_item_id bigint,
    
      order_id bigint,
    
      user_id bigint,
    
      product_id bigint,
    
      inventory_item_id bigint,
    
      status string COMMENT 'Normalized order item status.',
    
      created_at timestamp,
    
      item_revenue decimal(18, 2) COMMENT 'Non-negative sanity check.',
    
      product_name string,
    
      product_brand string,
    
      product_sku string,
    
      distribution_center_id bigint
    
    
  )

  
  using delta
  
  
  
  
  
  comment 'Order item fact at order_item_id grain.'
  

  