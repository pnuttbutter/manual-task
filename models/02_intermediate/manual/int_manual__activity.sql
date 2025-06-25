select 
    al.subscription_id, 
    al.customer_id,
    al.from_date,
    al.to_date,

    --generates a range using from & to date - note that ranges are not inclusive at the range end, hence the addition of 1 day
    range(al.from_date, date_add(al.to_date, interval 1 day)) as subscription_range,

    --subscription_duration_calculation
    date_diff(al.to_date, al.from_date, day) as duration_days,

    --returns true if subscription is active on the date of pipeline execution
    al.to_date >= current_date() and al.from_date <= current_date() as is_active,

from 
    {{ ref("stg_manual__activity") }}  al
