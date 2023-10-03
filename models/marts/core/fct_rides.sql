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
        ((2 + 1.75 + ride_distance/1609.34 * 1.15 + ride_duration/60 * 0.22)
        * (ride_prime_time/100+1)) as raw_total

    from rides
),

final as (

    select
        ride_id,
        (ride_distance/1609.34 * 1.15) as distance_price,
        (ride_duration/60 * 0.22) as duration_price,
        (ride_prime_time/100+1) as prime_time_multiplier,
        case 
             when (ROUND(raw_total,2)) <= 5 then 5.00
             when (ROUND(raw_total,2)) < 400 AND
                  (ROUND(raw_total,2)) > 5 then
                  (ROUND(raw_total,2))
             when (ROUND(raw_total,2)) >= 400 then 400.00
        end
        as total

    from est_cost
)

select * from final