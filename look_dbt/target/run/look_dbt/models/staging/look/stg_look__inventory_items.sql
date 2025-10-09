-- back compat for old kwarg name
  
  
  
  
  
  
      
          
          
      
  

    merge
    into
        sujeet_data_analytics_workspace.silver_dev.stg_look__inventory_items as DBT_INTERNAL_DEST
    using
        stg_look__inventory_items__dbt_tmp as DBT_INTERNAL_SOURCE
    on
        
              DBT_INTERNAL_SOURCE.id <=> DBT_INTERNAL_DEST.id
          
    when matched
        then update set
            `id` = DBT_INTERNAL_SOURCE.`id`, `product_id` = DBT_INTERNAL_SOURCE.`product_id`, `created_at` = DBT_INTERNAL_SOURCE.`created_at`, `sold_at` = DBT_INTERNAL_SOURCE.`sold_at`, `unit_cost` = DBT_INTERNAL_SOURCE.`unit_cost`, `product_category` = DBT_INTERNAL_SOURCE.`product_category`, `product_name` = DBT_INTERNAL_SOURCE.`product_name`, `product_brand` = DBT_INTERNAL_SOURCE.`product_brand`, `retail_price` = DBT_INTERNAL_SOURCE.`retail_price`, `product_department` = DBT_INTERNAL_SOURCE.`product_department`, `product_sku` = DBT_INTERNAL_SOURCE.`product_sku`, `product_distribution_center_id` = DBT_INTERNAL_SOURCE.`product_distribution_center_id`, `ingest_ts_utc` = DBT_INTERNAL_SOURCE.`ingest_ts_utc`, `_ingest_date` = DBT_INTERNAL_SOURCE.`_ingest_date`
    when not matched
        then insert
            (`id`, `product_id`, `created_at`, `sold_at`, `unit_cost`, `product_category`, `product_name`, `product_brand`, `retail_price`, `product_department`, `product_sku`, `product_distribution_center_id`, `ingest_ts_utc`, `_ingest_date`) VALUES (DBT_INTERNAL_SOURCE.`id`, DBT_INTERNAL_SOURCE.`product_id`, DBT_INTERNAL_SOURCE.`created_at`, DBT_INTERNAL_SOURCE.`sold_at`, DBT_INTERNAL_SOURCE.`unit_cost`, DBT_INTERNAL_SOURCE.`product_category`, DBT_INTERNAL_SOURCE.`product_name`, DBT_INTERNAL_SOURCE.`product_brand`, DBT_INTERNAL_SOURCE.`retail_price`, DBT_INTERNAL_SOURCE.`product_department`, DBT_INTERNAL_SOURCE.`product_sku`, DBT_INTERNAL_SOURCE.`product_distribution_center_id`, DBT_INTERNAL_SOURCE.`ingest_ts_utc`, DBT_INTERNAL_SOURCE.`_ingest_date`)

