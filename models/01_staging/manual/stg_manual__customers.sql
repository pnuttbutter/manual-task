with 
    source as 
    (
        select * from {{ source("manual", "customers") }}
    ),

    cleaned as
    (

        select
            safe_cast(customer_id as int) as customer_id,
            lower(trim(customer_country)) as country, 
        from source

    )

select customer_id, country
from cleaned
where customer_id is not null
