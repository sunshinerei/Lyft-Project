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
        30.437*(driver_rides.revenue/TIMESTAMP_DIFF(driver_rides.most_recent_ride_date, drivers.driver_onboard_date, day)) as avg_revenue_month,
        5.115151515151515 as lifetime_churn
            --avg lifetime of driver based on churn (calculated based on # of drivers active in March and # of drivers active in June)

    from driver_rides
    join drivers using (driver_id)

),

final as (

    select
        drivers.driver_id,
        drivers.driver_onboard_date,
        driver_rides.first_ride_date,
        driver_rides.most_recent_ride_date,
        coalesce(driver_rides.number_of_rides, 0) as number_of_rides,
        coalesce(driver_rides.revenue, 0) as revenue,
        TIMESTAMP_DIFF(driver_rides.most_recent_ride_date, drivers.driver_onboard_date, day) as tenure,
        driver_revenue.lifetime_churn,
        driver_revenue.avg_revenue_month,
        ROUND(driver_revenue.lifetime_churn * driver_revenue.avg_revenue_month, 2) as lifetime_value
        
    from drivers
    
    left join driver_rides using (driver_id)
    left join driver_revenue using (driver_id)

)

select * from final
ORDER BY avg_revenue_month