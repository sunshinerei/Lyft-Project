with source as (
    select *
    from {{ source('lyft', 'drivers') }}
),

staged as (
    select
        driver_id,
        driver_onboard_date
    from source
)

select * from staged
