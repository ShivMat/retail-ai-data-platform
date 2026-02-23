select
  order_date,
  product_id,
  product_name,
  sum(quantity) as total_qty,
  round(sum(revenue)::numeric, 2) as total_revenue
from {{ ref('stg_orders') }}
group by 1,2,3
order by 1,2
