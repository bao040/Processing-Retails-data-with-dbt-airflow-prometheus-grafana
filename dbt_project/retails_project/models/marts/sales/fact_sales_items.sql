{{ config(
    materialized='table'
) }}

with items as (
    select * from {{ ref('int_sales_items') }}
),

transactions as (
    select * from {{ ref('int_sales_transactions') }}
),

products as (
    select * from {{ ref('int_products') }}
),

customers as (
    select * from {{ ref('int_customers') }}
),

stores as (
    select * from {{ ref('int_stores') }}
),

employees as (
    select * from {{ ref('int_employees') }}
)

select
    i.item_id,
    trs.transaction_id,
    trs.transaction_date,

    -- Dimensions foreign keys
    p.product_id,
    c.customer_id,
    s.store_id,
    e.employee_id,

    -- Metrics
    i.quantity,
    i.unit_price,
    i.discount,
    i.tax,
    i.line_total as revenue,

    -- Extra calculation: gross before discount/tax
    (i.quantity * i.unit_price) as gross_amount

from items i
left join transactions trs on i.transaction_id = trs.transaction_id
left join products p on i.product_id = p.product_id
left join customers c on trs.customer_id = c.customer_id
left join stores s on trs.store_id = s.store_id
left join employees e on trs.employee_id = e.employee_id
