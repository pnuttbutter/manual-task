with
    source as 
    (

        select * from {{ source('manual','acq_orders') }}

    ),
    cleaned as
    (

        select 
            safe_cast(customer_id as int) as customer_id,
            lower(trim(taxonomy_business_category_group)) as acquisition_channel,
        from source
        
    )

select 
    customer_id, 
    acquisition_channel,
from 
    cleaned
where 
    customer_id is not null