with source as (
    select *
    from {{ source('lyft', 'rides') }}
),

staged as (
    select
        driver_id,
        ride_id,
        cast(ride_distance as decimal) as ride_distance,
        cast(ride_duration as decimal) as ride_duration,
        cast(ride_prime_time as decimal) as ride_prime_time
    from source
)

select * from staged