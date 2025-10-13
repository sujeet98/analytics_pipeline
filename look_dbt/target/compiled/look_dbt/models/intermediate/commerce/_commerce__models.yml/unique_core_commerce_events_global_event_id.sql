
    
    

select
    global_event_id as unique_field,
    count(*) as n_records

from sujeet_data_analytics_workspace.silver_dev.core_commerce_events
where global_event_id is not null
group by global_event_id
having count(*) > 1


