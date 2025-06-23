with 
    source as 
    ( 
        select * from {{ source("manual", "activity") }}
    ),

    cleaned as
    (

        select
            safe_cast(customer_id as int) as customer_id,
            safe_cast(subscription_id as int) as subscription_id,
            safe_cast(from_date as date) as from_date, 
            safe_cast(to_date as date) as to_date, 
        from source

    )

select customer_id, subscription_id, from_date, to_date
from cleaned
where customer_id is not null
    and subscription_id is not null
    and from_date is not null
    and to_date is not null