WITH
rides as (

    select * from {{ ref('stg_rides') }}

),

events as (

    select * from {{ ref('stg_events') }}

),

fact_rides as (

    select 
        driver_id,
        ROUND(SUM(total), 2) as lifetime_value
    
    from {{ ref('fct_rides') }}
    
    GROUP BY driver_id

),

ride_events as (

    select
        events.ride_id,
        events.event,
        events.timestamp,
        (-1 * (events.seconds - LEAD(events.seconds, 1) OVER
            (PARTITION BY events.ride_id ORDER BY events.timestamp)))
            AS event_length_seconds
    from events
    join rides
      on events.ride_id = rides.ride_id

)

select *
from ride_events