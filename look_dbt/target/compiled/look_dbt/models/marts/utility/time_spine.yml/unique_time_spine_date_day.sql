
    
    

select
    date_day as unique_field,
    count(*) as n_records

from sujeet_data_analytics_workspace.gold_dev.time_spine
where date_day is not null
group by date_day
having count(*) > 1


