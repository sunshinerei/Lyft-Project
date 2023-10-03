with drivers as (

    select * from {{ ref('stg_drivers') }}

),

rides as (

    select * from {{ ref('stg_rides') }}

),

events as (

    select * from {{ ref('stg_events') }}

),

fact_rides as (

    select * from {{ ref('fct_rides') }}

),

fact_events as (

    select * from {{ ref('fct_events') }}

),

driver_rides as (

    select
        rides.driver_id,

        min(events.timestamp) as first_ride_date,
        max(events.timestamp) as most_recent_ride_date,
        count(rides.ride_id) as number_of_rides,
        ROUND(SUM(fact_rides.total), 2) as lifetime_value

    from fact_rides
    join rides using (ride_id)
    join events using (ride_id)


    group by 1

),

final as (

    select
        drivers.driver_id,
        drivers.driver_onboard_date,
        driver_rides.first_ride_date,
        driver_rides.most_recent_ride_date,
        coalesce(driver_rides.number_of_rides, 0) as number_of_rides,
        coalesce(driver_rides.lifetime_value, 0) as lifetime_value

    from drivers
    
    left join driver_rides using (driver_id)

)

select * from final