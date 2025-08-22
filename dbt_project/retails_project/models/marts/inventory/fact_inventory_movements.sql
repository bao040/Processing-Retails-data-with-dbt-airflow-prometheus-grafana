{{ config(
    materialized='table'
) }}

with inventory as (
    select * from {{ ref('int_inventory') }}
),

movements as (
    select * from {{ ref('int_stock_movements') }}
),

shipments as (
    select * from {{ ref('int_shipments') }}
),

purchase_orders as (
    select * from {{ ref('int_purchase_orders') }}
)

select
    m.movement_id,
    m.product_id,
    m.store_id,
    m.movement_date,

    -- Join with shipments (inbound logistics)
    s.shipment_id,
    s.shipped_date,
    s.received_date,
    (s.received_date - s.shipped_date) as delivery_days,

    -- Join with purchase orders (procurement info)
    po.order_id,
    po.supplier_id,
    po.order_date,

    -- Inventory status at the time
    inv.quantity as current_quantity,
    inv.status as inventory_status,

    -- Metrics: stock movement classification
    case when m.movement_type = 'IN' then m.quantity else 0 end as stock_in,
    case when m.movement_type = 'OUT' then m.quantity else 0 end as stock_out,
    case when m.movement_type = 'TRANSFER' then m.quantity else 0 end as stock_transfer

from movements m
left join inventory inv 
       on m.product_id = inv.product_id 
      and m.store_id = inv.store_id
left join shipments s 
       on m.store_id = s.store_id 
      and m.movement_date between s.shipped_date and s.received_date
left join purchase_orders po 
       on s.order_id = po.order_id
