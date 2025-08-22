{{ config(
    materialized='table'
) }}

with sales_data as (
    select
        st.transaction_id,
        st.customer_id,
        st.store_id,
        st.transaction_date,
        st.total_amount,
        si.item_id,
        si.product_id,
        si.quantity,
        si.unit_price,
        si.discount,
        si.tax,
        si.line_total,
        dr.rule_id as discount_rule_id,
        dr.discount_type,
        dr.value as discount_value,
        dr.status as discount_status,
        -- Generate a unique sales key combining transaction_id and item_id
        concat(st.transaction_id, '-', si.item_id) as sales_key
    from {{ ref('int_sales_transactions') }} st
    join {{ ref('int_sales_items') }} si
        on st.transaction_id = si.transaction_id
    left join {{ ref('int_discount_rules') }} dr
        on si.product_id = dr.product_id
        and st.transaction_date between dr.valid_from and dr.valid_to
    where st.total_amount is not null
        and si.line_total is not null
),

aggregated_sales as (
    select
        sales_key,
        transaction_id,
        customer_id,
        store_id,
        product_id,
        transaction_date,
        sum(quantity) as total_quantity,
        sum(line_total) as total_line_amount,
        avg(discount) as avg_discount_applied,
        sum(tax) as total_tax,
        discount_rule_id,
        discount_type,
        discount_value,
        discount_status
    from sales_data
    group by
        sales_key,
        transaction_id,
        customer_id,
        store_id,
        product_id,
        transaction_date,
        discount_rule_id,
        discount_type,
        discount_value,
        discount_status
)

select
    s.sales_key,
    s.transaction_id,
    s.customer_id,
    s.store_id,
    s.product_id,
    s.transaction_date,
    s.total_quantity,
    s.total_line_amount,
    s.avg_discount_applied,
    s.total_tax,
    s.discount_rule_id,
    s.discount_type,
    s.discount_value,
    s.discount_status,
    c.loyalty_program_id,
    p.category_id,
    p.brand_id,
    p.season,
    p.product_age_days
from aggregated_sales s
join {{ ref('int_customers') }} c
    on s.customer_id = c.customer_id
join {{ ref('int_products') }} p
    on s.product_id = p.product_id
