with source as (
    select *
    from {{ source('lyft', 'rides_events') }}
),

staged as (
    select
        CONCAT(ride_id, event) as unique_id,
        ride_id,
        event,
        timestamp
    from source
)

select * from staged