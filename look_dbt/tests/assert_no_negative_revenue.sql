select *
from {{ ref('orders') }}
where order_gross_revenue < 0
