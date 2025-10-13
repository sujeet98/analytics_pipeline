
    
  create or replace table sujeet_data_analytics_workspace.silver_dev.stg_look__order_items
  
  (
    
      id bigint COMMENT 'Natural key of the line item.',
    
      order_id bigint COMMENT 'FK to orders.order_id',
    
      user_id bigint COMMENT 'FK to users.id (denormalized on the item row).',
    
      product_id bigint COMMENT 'FK to products.product_id',
    
      inventory_item_id bigint COMMENT 'FK to inventory_items.id (unit-level).',
    
      item_status string,
    
      created_at timestamp,
    
      shipped_at timestamp,
    
      delivered_at timestamp,
    
      returned_at timestamp,
    
      sale_price double,
    
      ingest_ts_utc timestamp,
    
      created_date date
    
    
  )

  
  using delta
  
  partitioned by (created_date)
  
  
  
  comment 'Staging (deduped) order line items; latest by ingest_ts_utc per id.'
  

  