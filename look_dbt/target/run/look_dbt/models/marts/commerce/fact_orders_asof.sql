
    
  create or replace table sujeet_data_analytics_workspace.gold_dev.fact_orders_asof
  
  (
    
      order_id bigint,
    
      customer_sk string,
    
      order_status string,
    
      order_date date,
    
      created_at timestamp,
    
      shipped_at timestamp,
    
      delivered_at timestamp,
    
      returned_at timestamp,
    
      num_of_item int
    
    
  )

  
  using delta
  
  partitioned by (order_date)
  
  
  
  comment 'Order lifecycle (AS-OF). Grain: 1 row per order_id. customer_sk resolved at created_at with earliest-version fallback.
'
  

  