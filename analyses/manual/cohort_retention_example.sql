--retention from initial cohort example

with cohort as 
(
  --cohort of customers 
  --in this example customers acquired in december 2023 from brazil
  select 
    customer_id
  from 
    {{ ref("dim_manual__customer")}}
  where  
    range_contains(range(date '2023-12-01', date '2024-01-01'), acquisition_date)
    and country = 'brazil'
)
--defines month ranges for calculation of retention
, retention_range as 
(
  select 
    range_start(month_range) as from_date, 
    range_end(month_range) as to_date,
    month_range,
  from 
    unnest(generate_range_array(range(date '2024-01-01', date '2025-01-01'),interval 1 month)) month_range
)

select 
  format_date('%m-%Y',rr.from_date) as retention_month,
  count(distinct c.customer_id) as count_cohort_customer,
  count(distinct ma.customer_id) as count_retention_customer,
  round(count(distinct ma.customer_id) / count(distinct c.customer_id),3) as retention_rate,
from cohort c
  cross join retention_range rr
  left join {{ref("fact_manual__activity")}} ma
    on c.customer_id = ma.customer_id
      and range_overlaps(ma.subscription_range , rr.month_range)
group by 
  1
order by 
  1