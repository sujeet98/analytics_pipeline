{{ config(materialized='view', tags=['marts','dim']) }}

select *
from {{ ref('dim_customer') }}
where is_current = true;
