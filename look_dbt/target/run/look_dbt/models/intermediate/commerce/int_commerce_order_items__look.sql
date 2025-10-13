
    
  create or replace table sujeet_data_analytics_workspace.silver_dev.int_commerce_order_items__look
  
  (
    
      order_item_id bigint COMMENT 'Natural key of the line item.',
    
      order_id bigint COMMENT 'FK to orders.order_id',
    
      user_id bigint COMMENT 'FK to users.id (denormalized on the item row).',
    
      product_id bigint COMMENT 'FK to products.product_id',
    
      inventory_item_id bigint COMMENT 'FK to inventory_items.id (unit-level).',
    
      item_status string,
    
      created_at timestamp,
    
      shipped_at timestamp,
    
      delivered_at timestamp,
    
      returned_at timestamp,
    
      item_date date,
    
      created_date date,
    
      sale_price double,
    
      source_system string,
    
      canonical_updated_at timestamp,
    
      ingest_ts_utc timestamp
    
    
  )

  
  using delta
  
  partitioned by (created_date)
  
  
  
  
  

  