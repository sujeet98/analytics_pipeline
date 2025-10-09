



with tgt_max as (
  select
                     timestamp('1900-01-01')  as max_ts
  from  (select 1) _ 
),
unioned as (
  select * from sujeet_data_analytics_workspace.silver_dev.int_commerce_events__look
  where canonical_updated_at >= dateadd(day, -2, (select max_ts from tgt_max))
)
select * from unioned;