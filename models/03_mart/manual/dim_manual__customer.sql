{{ config (
    materialized='table',
    primary_key='customer_id'
)}}

select
    c.customer_id,
    c.acquisition_channel,
    c.country,
    c.acquisition_date,
    c.is_active,
from 
    {{ref("int_manual__customers")}} c