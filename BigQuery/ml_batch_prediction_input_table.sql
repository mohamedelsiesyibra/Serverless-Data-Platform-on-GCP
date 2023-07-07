with 

input_table as(

select
  transaction_date,
  product_name,
  null as total_sales,
  weather_condition
from `tokyo-micron-390719.sales.ml-table`
where transaction_date = current_date()

)

select * from input_table