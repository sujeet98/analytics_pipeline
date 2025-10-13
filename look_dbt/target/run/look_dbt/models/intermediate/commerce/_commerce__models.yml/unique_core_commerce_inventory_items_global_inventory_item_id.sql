
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    global_inventory_item_id as unique_field,
    count(*) as n_records

from sujeet_data_analytics_workspace.silver_dev.core_commerce_inventory_items
where global_inventory_item_id is not null
group by global_inventory_item_id
having count(*) > 1



  
  
      
    ) dbt_internal_test