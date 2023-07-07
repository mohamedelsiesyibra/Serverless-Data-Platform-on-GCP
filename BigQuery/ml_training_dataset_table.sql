with 

sales as(

  select
    transaction_date,
    store_id,
    product_name,
    sum(quantity) as total_sales
  from `PROJECT_ID.DATASET_NAME.TABLE_NAME`
  group by 1, 2, 3

),

joining_sales_with_weather as(

  select
    s.transaction_date,
    s.store_id,
    s.product_name,
    s.total_sales,
    w.weather_condition
  from sales s
  inner join `PROJECT_ID.DATASET_NAME.TABLE_NAME` w
  on s.transaction_date = w.day

)

select 
  * 
from joining_sales_with_weather
