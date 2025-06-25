{{ config (
    materialized='table',
)}}

select
    ma.subscription_id, 
    ma.customer_id,
    ma.from_date,
    ma.to_date,
    ma.subscription_range,
    ma.duration_days,
    ma.is_active,
from 
    {{ref("int_manual__activity")}} ma