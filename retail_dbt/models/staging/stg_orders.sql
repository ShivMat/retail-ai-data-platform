select
  order_id,
  order_date,
  product_id,
  product_name,
  quantity,
  unit_price,
  customer_id,
  revenue
from staging.stg_orders
