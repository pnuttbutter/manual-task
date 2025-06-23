with subscription_lag as 
(
    select
        a.subscription_id,
        a.customer_id,
        a.from_date, 
        a.to_date,
        --returns the to_date from the previous subscription instance, null if none exists
        lag(a.to_date) over (partition by a.customer_id, a.subscription_id order by a.to_date) as previous_to_date
    from {{ ref("stg_manual__activity") }} a
)

select 
    sl.subscription_id, 
    sl.customer_id,
    sl.from_date,
    sl.to_date,

    --if previous subscription instance ended within n days of the start of this subscription, then considered to be a renewal
    case 
        when sl.previous_to_date is not null
            --and date_diff(sl.from_date, sl.previous_to_date, DAY ) < 3
        then 'renewal'
        else 'activation'
    end as subscription_type,

    date_diff(sl.from_date, sl.previous_to_date, DAY ) as days_elapsed,
    
    to_date >= current_date() and from_date <= current_date() as is_active

from subscription_lag sl