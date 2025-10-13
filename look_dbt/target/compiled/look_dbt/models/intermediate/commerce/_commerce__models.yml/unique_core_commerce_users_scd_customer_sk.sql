
    
    

select
    customer_sk as unique_field,
    count(*) as n_records

from sujeet_data_analytics_workspace.silver_dev.core_commerce_users_scd
where customer_sk is not null
group by customer_sk
having count(*) > 1


