with
    customer_aquisition_date
    (
        select a.customer_id, min(a.from_date) as aquisition_date
        from {{ ref("stg_manual__activity") }} a
        group by 1
    ),
    customer_active
    (
        select a.customer_id
        from {{ ref("stg_manual__activity") }} a
        group by 1
        having max(to_date) >= current_date()
    )

select
    c.customer_id,
    c.aquisition_channel,
    ao.country as country,
    cad.aquisition_channel,
    ca.customer_id is not null as is_active
from {{ ref("stg_manual__customers") }} c
left join
    {{ ref("stg_manual__acquisition_orders") }} ao on c.customer_id = ao.customer_id
left join customer_aquisition_date cad on c.customer_id = cad.customer_id
left join customer_active ca on c.customer_id = ca.customer_id
