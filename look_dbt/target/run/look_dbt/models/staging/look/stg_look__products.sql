-- back compat for old kwarg name
  
  
  
  
  
  
      
          
          
      
  

    merge
    into
        sujeet_data_analytics_workspace.silver_dev.stg_look__products as DBT_INTERNAL_DEST
    using
        stg_look__products__dbt_tmp as DBT_INTERNAL_SOURCE
    on
        
              DBT_INTERNAL_SOURCE.product_id <=> DBT_INTERNAL_DEST.product_id
          
    when matched
        then update set
            `product_id` = DBT_INTERNAL_SOURCE.`product_id`, `unit_cost` = DBT_INTERNAL_SOURCE.`unit_cost`, `category` = DBT_INTERNAL_SOURCE.`category`, `product_name` = DBT_INTERNAL_SOURCE.`product_name`, `brand` = DBT_INTERNAL_SOURCE.`brand`, `retail_price` = DBT_INTERNAL_SOURCE.`retail_price`, `department` = DBT_INTERNAL_SOURCE.`department`, `sku` = DBT_INTERNAL_SOURCE.`sku`, `distribution_center_id` = DBT_INTERNAL_SOURCE.`distribution_center_id`, `ingest_ts_utc` = DBT_INTERNAL_SOURCE.`ingest_ts_utc`, `_ingest_date` = DBT_INTERNAL_SOURCE.`_ingest_date`
    when not matched
        then insert
            (`product_id`, `unit_cost`, `category`, `product_name`, `brand`, `retail_price`, `department`, `sku`, `distribution_center_id`, `ingest_ts_utc`, `_ingest_date`) VALUES (DBT_INTERNAL_SOURCE.`product_id`, DBT_INTERNAL_SOURCE.`unit_cost`, DBT_INTERNAL_SOURCE.`category`, DBT_INTERNAL_SOURCE.`product_name`, DBT_INTERNAL_SOURCE.`brand`, DBT_INTERNAL_SOURCE.`retail_price`, DBT_INTERNAL_SOURCE.`department`, DBT_INTERNAL_SOURCE.`sku`, DBT_INTERNAL_SOURCE.`distribution_center_id`, DBT_INTERNAL_SOURCE.`ingest_ts_utc`, DBT_INTERNAL_SOURCE.`_ingest_date`)

