
    
  create or replace table sujeet_data_analytics_workspace.gold_dev.fact_order_items_current
  
  (
    
      order_item_id bigint,
    
      order_id bigint,
    
      customer_sk string,
    
      product_sk string,
    
      dc_sk string,
    
      created_at timestamp,
    
      created_date date,
    
      item_date date COMMENT 'DATE(COALESCE(delivered_at, created_at)) provided by int layer.',
    
      shipped_at timestamp,
    
      delivered_at timestamp,
    
      returned_at timestamp,
    
      sale_price decimal(10, 0)
    
    
  )

  
  using delta
  
  partitioned by (created_date)
  
  
  
  comment 'Order item fact (CURRENT). Grain: 1 row per order_item_id. SKs resolved to current dims (customer, product, DC). Uses same grain/columns as AS-OF for easy swapping.
'
  

  