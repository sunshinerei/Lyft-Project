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
        min(DATE_TRUNC(events.timestamp, day)) as first_ride_date,
        max(DATE_TRUNC(events.timestamp, day)) as most_recent_ride_date,
        count(rides.ride_id) as number_of_rides,
        ROUND(SUM(fact_rides.total), 2) as revenue

    from fact_rides
    join rides using (ride_id)
    join events using (ride_id)

    group by 1

),

driver_revenue as (
        
    select
        driver_id,
        revenue/TIMESTAMP_DIFF(most_recent_ride_date, first_ride_date, day) as revenue_day,

        (select SUM(revenue) from driver_rides)
        /(select SUM(TIMESTAMP_DIFF(most_recent_ride_date, first_ride_date, day)) from driver_rides) as avg_revenue_day,

        (select SUM(TIMESTAMP_DIFF(most_recent_ride_date, first_ride_date, day)) from driver_rides)
        /(select COUNT(driver_id) from driver_rides) as avg_customer_lifespan

    from driver_rides

),

final as (

    select
        drivers.driver_id,
        drivers.driver_onboard_date,
        driver_rides.first_ride_date,
        driver_rides.most_recent_ride_date,
        coalesce(driver_rides.number_of_rides, 0) as number_of_rides,
        coalesce(driver_rides.revenue, 0) as revenue,
        TIMESTAMP_DIFF(driver_rides.most_recent_ride_date, driver_rides.first_ride_date, day) as customer_lifespan,
        driver_revenue.revenue_day,
        driver_revenue.avg_customer_lifespan,
        driver_revenue.avg_revenue_day,
        ROUND(driver_revenue.avg_customer_lifespan * driver_revenue.revenue_day, 2) as est_customer_lifetime_value,
        ROUND(driver_revenue.avg_customer_lifespan * driver_revenue.avg_revenue_day, 2) as avg_customer_lifetime_value
        
    from drivers
    
    left join driver_rides using (driver_id)
    left join driver_revenue using (driver_id)

)

select * from final