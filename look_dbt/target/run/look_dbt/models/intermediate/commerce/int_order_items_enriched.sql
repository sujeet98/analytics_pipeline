
    
  create or replace table sujeet_data_analytics_workspace.silver_dev.int_order_items_enriched
  
  (
    
      order_item_id bigint,
    
      order_id bigint,
    
      user_id bigint,
    
      product_id bigint,
    
      inventory_item_id bigint,
    
      item_status string,
    
      sale_price decimal(18, 2) COMMENT 'Nullable; when present it must be >= 0.',
    
      item_created_at timestamp,
    
      inv_product_id bigint,
    
      product_distribution_center_id bigint,
    
      product_name string,
    
      product_brand string,
    
      product_category string,
    
      product_department string,
    
      retail_price decimal(18, 2),
    
      product_sku string,
    
      distribution_center_id bigint,
    
      distribution_center_name string,
    
      distribution_center_latitude double,
    
      distribution_center_longitude double
    
    
  )

  
  using delta
  
  
  
  
  
  comment 'Order items enriched with product/DC attributes. Row grain = order_item_id.'
  

  