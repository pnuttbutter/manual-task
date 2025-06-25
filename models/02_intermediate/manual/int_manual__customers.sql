with
    --returns customer_id and date of earliest subscription
    customer_acquisition_date as 
    (
        select a.customer_id, min(a.from_date) as acquisition_date
        from {{ ref("stg_manual__activity") }} a
        group by 1
    ),
    --returns customer_id and boolean to indicate if a customer has at leaast one current subscription on the date of pipeline execution
    customer_active as 
    (
        select a.customer_id
        from {{ ref("stg_manual__activity") }} a
        group by 1
        having max(to_date) >= current_date()
    )

select
    c.customer_id,
    ao.acquisition_channel,
    c.country as country,
    cad.acquisition_date,
    ca.customer_id is not null as is_active,
from {{ ref("stg_manual__customers") }} c
    left join {{ ref("stg_manual__acquisition_orders") }} ao on c.customer_id = ao.customer_id
    left join customer_acquisition_date cad on c.customer_id = cad.customer_id
    left join customer_active ca on c.customer_id = ca.customer_id
