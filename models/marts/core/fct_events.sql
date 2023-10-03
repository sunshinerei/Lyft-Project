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
        ROW_NUMBER() OVER (PARTITION BY event.ride_id 
                           ORDER BY event.timestamp)
                     AS event_sequence
    from events
    join rides
      on events.ride_id = rides.ride_id


    group by 1

)

select * from ride_events