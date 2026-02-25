{{ config(materialized='table') }}

with customers as (
    select
        DimCustomersKey as customer_key,
        passenger_id,
        name as customer_name,
        gender,
        nationality
    from workspace.gold.dimcustomers
),

bookings as (
    select
        DimCustomersKey as customer_key,
        count(*) as total_bookings,
        min(booking_date) as first_booking_date,
        max(booking_date) as last_booking_date
    from workspace.gold.factbookings
    group by DimCustomersKey
)

select
    c.customer_key,
    c.passenger_id,
    c.customer_name,
    c.gender,
    c.nationality,
    b.total_bookings,
    b.first_booking_date,
    b.last_booking_date
from customers c
left join bookings b
    on c.customer_key = b.customer_key
