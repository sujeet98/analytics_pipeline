{{ config(materialized='table') }}

-- Purpose: Calendar dimension (utility) for reporting and joins
with d as (
  select explode(sequence(date'2018-01-01', date'2032-12-31', interval 1 day)) as date_day
)
select
  date_day,
  year(date_day)  as year,
  month(date_day) as month,
  day(date_day)   as day,
  weekofyear(date_day) as week_of_year,
  quarter(date_day)    as quarter,
  date_format(date_day, 'yyyy-MM') as year_month
from d
