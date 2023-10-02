with source as (
    select *
    from {{ source('lyft', 'events') }}
),

staged as (
    select
        ride_id,
        event,
        timestamp
    from source
)

select * from staged