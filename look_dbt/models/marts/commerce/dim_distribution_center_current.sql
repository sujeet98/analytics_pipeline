{{ config(materialized='view', tags=['marts','dim']) }}

select *
from {{ ref('dim_distribution_center') }}
where is_current = true;
