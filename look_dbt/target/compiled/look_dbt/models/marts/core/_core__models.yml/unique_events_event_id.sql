
    
    

select
    event_id as unique_field,
    count(*) as n_records

from sujeet_data_analytics_workspace.gold_dev.events
where event_id is not null
group by event_id
having count(*) > 1


