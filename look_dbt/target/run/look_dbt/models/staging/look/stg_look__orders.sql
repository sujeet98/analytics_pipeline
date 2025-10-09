-- back compat for old kwarg name
  
  
  
  
  
  
      
          
          
      
  

    merge
    into
        sujeet_data_analytics_workspace.silver_dev.stg_look__orders as DBT_INTERNAL_DEST
    using
        stg_look__orders__dbt_tmp as DBT_INTERNAL_SOURCE
    on
        
              DBT_INTERNAL_SOURCE.order_id <=> DBT_INTERNAL_DEST.order_id
          
    when matched
        then update set
            `order_id` = DBT_INTERNAL_SOURCE.`order_id`, `user_id` = DBT_INTERNAL_SOURCE.`user_id`, `order_status` = DBT_INTERNAL_SOURCE.`order_status`, `user_gender` = DBT_INTERNAL_SOURCE.`user_gender`, `created_at` = DBT_INTERNAL_SOURCE.`created_at`, `shipped_at` = DBT_INTERNAL_SOURCE.`shipped_at`, `delivered_at` = DBT_INTERNAL_SOURCE.`delivered_at`, `returned_at` = DBT_INTERNAL_SOURCE.`returned_at`, `num_of_item` = DBT_INTERNAL_SOURCE.`num_of_item`, `ingest_ts_utc` = DBT_INTERNAL_SOURCE.`ingest_ts_utc`, `_ingest_date` = DBT_INTERNAL_SOURCE.`_ingest_date`
    when not matched
        then insert
            (`order_id`, `user_id`, `order_status`, `user_gender`, `created_at`, `shipped_at`, `delivered_at`, `returned_at`, `num_of_item`, `ingest_ts_utc`, `_ingest_date`) VALUES (DBT_INTERNAL_SOURCE.`order_id`, DBT_INTERNAL_SOURCE.`user_id`, DBT_INTERNAL_SOURCE.`order_status`, DBT_INTERNAL_SOURCE.`user_gender`, DBT_INTERNAL_SOURCE.`created_at`, DBT_INTERNAL_SOURCE.`shipped_at`, DBT_INTERNAL_SOURCE.`delivered_at`, DBT_INTERNAL_SOURCE.`returned_at`, DBT_INTERNAL_SOURCE.`num_of_item`, DBT_INTERNAL_SOURCE.`ingest_ts_utc`, DBT_INTERNAL_SOURCE.`_ingest_date`)

