with drivers as (

    select * from {{ ref('stg_drivers') }}

),

rides as (

    select * from {{ ref('stg_rides') }}

),

events as (

    select * from {{ ref('stg_events') }}

),

driver_rides as (

    select
        r.driver_id,

        min(e.timestamp) as first_ride_date,
        max(e.timestamp) as most_recent_ride_date,
        count(r.ride_id) as number_of_rides

    from events as e
    join rides as r
      on e.ride_id = r.ride_id


    group by 1

)

select * from driver_rides