-- back compat for old kwarg name
  
  
  
  
  
  
      
          
          
      
  

    merge
    into
        sujeet_data_analytics_workspace.silver_dev.stg_look__distribution_centers as DBT_INTERNAL_DEST
    using
        stg_look__distribution_centers__dbt_tmp as DBT_INTERNAL_SOURCE
    on
        
              DBT_INTERNAL_SOURCE.distribution_center_id <=> DBT_INTERNAL_DEST.distribution_center_id
          
    when matched
        then update set
            `distribution_center_id` = DBT_INTERNAL_SOURCE.`distribution_center_id`, `name` = DBT_INTERNAL_SOURCE.`name`, `latitude` = DBT_INTERNAL_SOURCE.`latitude`, `longitude` = DBT_INTERNAL_SOURCE.`longitude`, `ingest_ts_utc` = DBT_INTERNAL_SOURCE.`ingest_ts_utc`, `_ingest_date` = DBT_INTERNAL_SOURCE.`_ingest_date`
    when not matched
        then insert
            (`distribution_center_id`, `name`, `latitude`, `longitude`, `ingest_ts_utc`, `_ingest_date`) VALUES (DBT_INTERNAL_SOURCE.`distribution_center_id`, DBT_INTERNAL_SOURCE.`name`, DBT_INTERNAL_SOURCE.`latitude`, DBT_INTERNAL_SOURCE.`longitude`, DBT_INTERNAL_SOURCE.`ingest_ts_utc`, DBT_INTERNAL_SOURCE.`_ingest_date`)

