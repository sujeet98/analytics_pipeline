
    
    

select
    event_id as unique_field,
    count(*) as n_records

from sujeet_data_analytics_workspace.gold_dev.fact_events_current
where event_id is not null
group by event_id
having count(*) > 1


