{{ config(materialized='view', tags=['marts','dim']) }}

select *
from {{ ref('dim_product') }}
where is_current = true;
