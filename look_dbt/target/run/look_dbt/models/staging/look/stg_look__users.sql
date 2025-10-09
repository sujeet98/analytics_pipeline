-- back compat for old kwarg name
  
  
  
  
  
  
      
          
          
      
  

    merge
    into
        sujeet_data_analytics_workspace.silver_dev.stg_look__users as DBT_INTERNAL_DEST
    using
        stg_look__users__dbt_tmp as DBT_INTERNAL_SOURCE
    on
        
              DBT_INTERNAL_SOURCE.id <=> DBT_INTERNAL_DEST.id
          
    when matched
        then update set
            `id` = DBT_INTERNAL_SOURCE.`id`, `first_name` = DBT_INTERNAL_SOURCE.`first_name`, `last_name` = DBT_INTERNAL_SOURCE.`last_name`, `email` = DBT_INTERNAL_SOURCE.`email`, `age` = DBT_INTERNAL_SOURCE.`age`, `gender` = DBT_INTERNAL_SOURCE.`gender`, `state` = DBT_INTERNAL_SOURCE.`state`, `street_address` = DBT_INTERNAL_SOURCE.`street_address`, `postal_code` = DBT_INTERNAL_SOURCE.`postal_code`, `city` = DBT_INTERNAL_SOURCE.`city`, `country` = DBT_INTERNAL_SOURCE.`country`, `latitude` = DBT_INTERNAL_SOURCE.`latitude`, `longitude` = DBT_INTERNAL_SOURCE.`longitude`, `traffic_source` = DBT_INTERNAL_SOURCE.`traffic_source`, `created_at` = DBT_INTERNAL_SOURCE.`created_at`, `ingest_ts_utc` = DBT_INTERNAL_SOURCE.`ingest_ts_utc`, `_ingest_date` = DBT_INTERNAL_SOURCE.`_ingest_date`
    when not matched
        then insert
            (`id`, `first_name`, `last_name`, `email`, `age`, `gender`, `state`, `street_address`, `postal_code`, `city`, `country`, `latitude`, `longitude`, `traffic_source`, `created_at`, `ingest_ts_utc`, `_ingest_date`) VALUES (DBT_INTERNAL_SOURCE.`id`, DBT_INTERNAL_SOURCE.`first_name`, DBT_INTERNAL_SOURCE.`last_name`, DBT_INTERNAL_SOURCE.`email`, DBT_INTERNAL_SOURCE.`age`, DBT_INTERNAL_SOURCE.`gender`, DBT_INTERNAL_SOURCE.`state`, DBT_INTERNAL_SOURCE.`street_address`, DBT_INTERNAL_SOURCE.`postal_code`, DBT_INTERNAL_SOURCE.`city`, DBT_INTERNAL_SOURCE.`country`, DBT_INTERNAL_SOURCE.`latitude`, DBT_INTERNAL_SOURCE.`longitude`, DBT_INTERNAL_SOURCE.`traffic_source`, DBT_INTERNAL_SOURCE.`created_at`, DBT_INTERNAL_SOURCE.`ingest_ts_utc`, DBT_INTERNAL_SOURCE.`_ingest_date`)

