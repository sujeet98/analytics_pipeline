-- back compat for old kwarg name
  
  
  
  
  
  
      
          
          
      
  

    merge
    into
        sujeet_data_analytics_workspace.silver_dev.stg_look__order_items as DBT_INTERNAL_DEST
    using
        stg_look__order_items__dbt_tmp as DBT_INTERNAL_SOURCE
    on
        
              DBT_INTERNAL_SOURCE.id <=> DBT_INTERNAL_DEST.id
          
    when matched
        then update set
            `id` = DBT_INTERNAL_SOURCE.`id`, `order_id` = DBT_INTERNAL_SOURCE.`order_id`, `user_id` = DBT_INTERNAL_SOURCE.`user_id`, `product_id` = DBT_INTERNAL_SOURCE.`product_id`, `inventory_item_id` = DBT_INTERNAL_SOURCE.`inventory_item_id`, `item_status` = DBT_INTERNAL_SOURCE.`item_status`, `created_at` = DBT_INTERNAL_SOURCE.`created_at`, `shipped_at` = DBT_INTERNAL_SOURCE.`shipped_at`, `delivered_at` = DBT_INTERNAL_SOURCE.`delivered_at`, `returned_at` = DBT_INTERNAL_SOURCE.`returned_at`, `sale_price` = DBT_INTERNAL_SOURCE.`sale_price`, `ingest_ts_utc` = DBT_INTERNAL_SOURCE.`ingest_ts_utc`, `_ingest_date` = DBT_INTERNAL_SOURCE.`_ingest_date`
    when not matched
        then insert
            (`id`, `order_id`, `user_id`, `product_id`, `inventory_item_id`, `item_status`, `created_at`, `shipped_at`, `delivered_at`, `returned_at`, `sale_price`, `ingest_ts_utc`, `_ingest_date`) VALUES (DBT_INTERNAL_SOURCE.`id`, DBT_INTERNAL_SOURCE.`order_id`, DBT_INTERNAL_SOURCE.`user_id`, DBT_INTERNAL_SOURCE.`product_id`, DBT_INTERNAL_SOURCE.`inventory_item_id`, DBT_INTERNAL_SOURCE.`item_status`, DBT_INTERNAL_SOURCE.`created_at`, DBT_INTERNAL_SOURCE.`shipped_at`, DBT_INTERNAL_SOURCE.`delivered_at`, DBT_INTERNAL_SOURCE.`returned_at`, DBT_INTERNAL_SOURCE.`sale_price`, DBT_INTERNAL_SOURCE.`ingest_ts_utc`, DBT_INTERNAL_SOURCE.`_ingest_date`)

