-- back compat for old kwarg name
  
  
  
  
  
  
      
          
          
      
  

    merge
    into
        sujeet_data_analytics_workspace.gold_dev.orders as DBT_INTERNAL_DEST
    using
        orders__dbt_tmp as DBT_INTERNAL_SOURCE
    on
        
              DBT_INTERNAL_SOURCE.order_id <=> DBT_INTERNAL_DEST.order_id
          
    when matched
        then update set
            `order_id` = DBT_INTERNAL_SOURCE.`order_id`, `user_id` = DBT_INTERNAL_SOURCE.`user_id`, `status` = DBT_INTERNAL_SOURCE.`status`, `created_at` = DBT_INTERNAL_SOURCE.`created_at`, `item_count` = DBT_INTERNAL_SOURCE.`item_count`, `order_gross_revenue` = DBT_INTERNAL_SOURCE.`order_gross_revenue`
    when not matched
        then insert
            (`order_id`, `user_id`, `status`, `created_at`, `item_count`, `order_gross_revenue`) VALUES (DBT_INTERNAL_SOURCE.`order_id`, DBT_INTERNAL_SOURCE.`user_id`, DBT_INTERNAL_SOURCE.`status`, DBT_INTERNAL_SOURCE.`created_at`, DBT_INTERNAL_SOURCE.`item_count`, DBT_INTERNAL_SOURCE.`order_gross_revenue`)

