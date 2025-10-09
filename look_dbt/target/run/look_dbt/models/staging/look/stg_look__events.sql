-- back compat for old kwarg name
  
  
  
  
  
  
      
          
          
      
  

    merge
    into
        sujeet_data_analytics_workspace.silver_dev.stg_look__events as DBT_INTERNAL_DEST
    using
        stg_look__events__dbt_tmp as DBT_INTERNAL_SOURCE
    on
        
              DBT_INTERNAL_SOURCE.id <=> DBT_INTERNAL_DEST.id
          
    when matched
        then update set
            `id` = DBT_INTERNAL_SOURCE.`id`, `user_id` = DBT_INTERNAL_SOURCE.`user_id`, `sequence_number` = DBT_INTERNAL_SOURCE.`sequence_number`, `session_id` = DBT_INTERNAL_SOURCE.`session_id`, `event_ts` = DBT_INTERNAL_SOURCE.`event_ts`, `event_date` = DBT_INTERNAL_SOURCE.`event_date`, `ip_address` = DBT_INTERNAL_SOURCE.`ip_address`, `city` = DBT_INTERNAL_SOURCE.`city`, `state` = DBT_INTERNAL_SOURCE.`state`, `postal_code` = DBT_INTERNAL_SOURCE.`postal_code`, `browser` = DBT_INTERNAL_SOURCE.`browser`, `traffic_source` = DBT_INTERNAL_SOURCE.`traffic_source`, `uri` = DBT_INTERNAL_SOURCE.`uri`, `event_type` = DBT_INTERNAL_SOURCE.`event_type`, `ingest_ts_utc` = DBT_INTERNAL_SOURCE.`ingest_ts_utc`
    when not matched
        then insert
            (`id`, `user_id`, `sequence_number`, `session_id`, `event_ts`, `event_date`, `ip_address`, `city`, `state`, `postal_code`, `browser`, `traffic_source`, `uri`, `event_type`, `ingest_ts_utc`) VALUES (DBT_INTERNAL_SOURCE.`id`, DBT_INTERNAL_SOURCE.`user_id`, DBT_INTERNAL_SOURCE.`sequence_number`, DBT_INTERNAL_SOURCE.`session_id`, DBT_INTERNAL_SOURCE.`event_ts`, DBT_INTERNAL_SOURCE.`event_date`, DBT_INTERNAL_SOURCE.`ip_address`, DBT_INTERNAL_SOURCE.`city`, DBT_INTERNAL_SOURCE.`state`, DBT_INTERNAL_SOURCE.`postal_code`, DBT_INTERNAL_SOURCE.`browser`, DBT_INTERNAL_SOURCE.`traffic_source`, DBT_INTERNAL_SOURCE.`uri`, DBT_INTERNAL_SOURCE.`event_type`, DBT_INTERNAL_SOURCE.`ingest_ts_utc`)

