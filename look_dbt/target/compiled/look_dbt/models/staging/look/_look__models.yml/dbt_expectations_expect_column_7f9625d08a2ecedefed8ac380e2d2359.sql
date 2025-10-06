






    with grouped_expression as (
    select
        
        
    
  
( 1=1 and sale_price >= 0
)
 as expression


    from sujeet_data_analytics_workspace.silver_dev.stg_look__order_items
    

),
validation_errors as (

    select
        *
    from
        grouped_expression
    where
        not(expression = true)

)

select *
from validation_errors







