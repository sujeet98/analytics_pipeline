{{ config(materialized = 'table') }}

-- Day-grain calendar spine for MetricFlow / Semantic Layer
select
  cast(d.date_day as date) as date_day
from (
  {{ dbt_utils.date_spine(
       datepart   = 'day',
       start_date = "to_date('2000-01-01')",
       end_date   = "to_date('2100-01-01')"
     ) }}
) as d
