
    
  create or replace table sujeet_data_analytics_workspace.gold_dev.time_spine
  
  (
    
      date_day date COMMENT 'Calendar date at day grain.'
    
    
  )

  
  using delta
  
  
  
  
  
  comment 'Day-level date spine used by the dbt Semantic Layer/MetricFlow to generate complete time series and handle time-aware joins.
'
  

  