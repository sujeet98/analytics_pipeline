
    
  create or replace table sujeet_data_analytics_workspace.gold_dev.fact_order_items_asof
  
  (
    
      order_item_id bigint,
    
      order_id bigint,
    
      customer_sk string,
    
      product_sk string,
    
      dc_sk string COMMENT 'Nullable if product has no DC.',
    
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
  
  
  
  comment 'Order item fact (AS-OF). Grain: 1 row per order_item_id. SKs resolved at created_at with earliest-version fallback. Includes customer_sk, product_sk, and dc_sk (via product\'s DC).
'
  

  