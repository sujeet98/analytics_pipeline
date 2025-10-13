
      merge into `sujeet_data_analytics_workspace`.`snapshots`.`snap_users__look` as DBT_INTERNAL_DEST
    
      using snap_users__look__dbt_tmp as DBT_INTERNAL_SOURCE
    
    on DBT_INTERNAL_SOURCE.dbt_scd_id = DBT_INTERNAL_DEST.dbt_scd_id
    when matched
     
       and DBT_INTERNAL_DEST.dbt_valid_to is null
     
     and DBT_INTERNAL_SOURCE.dbt_change_type in ('update', 'delete')
        then update
        set dbt_valid_to = DBT_INTERNAL_SOURCE.dbt_valid_to

    when not matched
     and DBT_INTERNAL_SOURCE.dbt_change_type = 'insert'
        then insert *
    ;

  