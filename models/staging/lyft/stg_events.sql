with source as (
    select *
    from {{ source('lyft', 'events') }}
),

staged as (
    select
        ride_id,
        event,
        timestamp,
        UNIX_SECONDS(timestamp) as seconds
    from source
)

select * from staged