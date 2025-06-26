--retention over time
 
 --defines date range for calculation of retention
 with retention_range as 
 (
  select range(date '2023-12-01', date '2024-01-01') as retention_range
 )

 --count of distinct customers active at end of retention range
, start_range as
(
  select
    count(distinct a.customer_id) as count_customer
  from 
    {{ref("fact_manual__activity")}} a
    cross join retention_range rr
  where 
    range_contains(a.subscription_range, range_start(rr.retention_range))
)

 --count of distinct customers active at end of retention range
, end_range as
(
  select
    count(distinct a.customer_id) as count_customer
  from 
    {{ref("fact_manual__activity")}} a
    cross join retention_range rr
  where 
    range_contains(a.subscription_range, range_end(rr.retention_range))
)

--count of customers that were acquired in retention range
, new_customers as 
(  
  select 
    count(distinct c.customer_id) as count_customer
  from 
   {{ ref("dim_manual__customer")}} c
    cross join retention_range rr
  where 
    range_contains(rr.retention_range, c.acquisition_date)
)


select 
  rr.retention_range
  , nc.count_customer as new_customers
  , sr.count_customer as customers_active_at_range_start
  , er.count_customer as customers_active_at_range_end
  , round((er.count_customer - nc.count_customer) / sr.count_customer , 3) as retention_rate
from retention_range rr
  cross join start_range sr
  cross join end_range er
  cross join new_customers nc

