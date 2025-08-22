{{ config(
    materialized='table'
) }}

with dates as (
    select distinct
        transaction_date
    from {{  ref('int_sales_transactions') }}

),
--vhvhb
calendar as (
    select
        transaction_date as date_id,
        extract(year from transaction_date)::int as year,
        extract(quarter from transaction_date)::int as quarter,
        extract(month from transaction_date)::int as month,
        to_char(transaction_date, 'Month') as month_name,
        extract(day from transaction_date)::int as day_of_month,
        extract(dow from transaction_date)::int as day_of_week, -- 0 = Sunday, 6 = Saturday
        to_char(transaction_date, 'Day') as day_name,
        extract(week from transaction_date)::int as week_of_year,
        case
            when extract(dow from transaction_date) in (0,6) then true
            else false
        end as is_weekend
    from dates
)

select * from calendar