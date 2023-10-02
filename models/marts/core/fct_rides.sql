with drivers as (

    select * from {{ ref('stg_drivers') }}

),

rides as (

    select * from {{ ref('stg_rides') }}

),

events as (

    select * from {{ ref('stg_events') }}

),

est_cost as (
    select
        *,
        2 + 1.75 + ride_distance/1609.34 * 1.15 + ride_duration/60 * 0.22 as amount,
        ride_prime_time/100+1 as prime_time_multiplier

    from rides
),

final as (

    select
        ride_id,
        driver_id,
        ride_distance,
        ride_duration,
        prime_time_multiplier,
        case 
             when (ROUND(amount*prime_time_multiplier,2)) <= 5 then 5.00
             when (ROUND(amount*prime_time_multiplier,2)) < 400 AND
                  (ROUND(amount*prime_time_multiplier,2)) > 5 then
                  (ROUND(amount*prime_time_multiplier,2))
             when (ROUND(amount*prime_time_multiplier,2)) >= 400 then 400.00
        end
        as amount

    from est_cost
)

select * from final