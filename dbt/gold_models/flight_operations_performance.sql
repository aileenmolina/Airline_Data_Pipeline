{{ config(materialized='table') }}

with flights as (
    select
        DimFlightsKey as flight_key,
        flight_id,
        airline,
        origin,
        destination,
        flight_date
    from workspace.gold.dimflights
),

bookings as (
    select
        DimFlightsKey as flight_key,
        count(*) as total_bookings,
        sum(amount) as total_revenue
    from workspace.gold.factbookings
    group by DimFlightsKey
)

select
    f.flight_key,
    f.flight_id,
    f.airline,
    f.origin,
    f.destination,
    f.flight_date,
    b.total_bookings,
    b.total_revenue
from flights f
left join bookings b
    on f.flight_key = b.flight_key
