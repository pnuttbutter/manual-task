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

--returns all customers that have a customer id that is null, or cannot be safe-casted from string to int
select customer_id, country
from cleaned
where customer_id is null