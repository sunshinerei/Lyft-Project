with source as (
    select *
    from {{ source('lyft', 'rides') }}
),

staged as (
    select
        driver_id,
        ride_id,
        ride_distance,
        ride_duration,
        ride_prime_time
    from source
)

select * from staged