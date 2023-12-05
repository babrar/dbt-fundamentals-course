with orders as (
    select * from {{ ref("stg_orders") }}
),

payments as (
    select * from {{ ref("stg_payments") }}
),

order_amount as (
    select
        o.order_id,
        sum(
            case when p.status = 'success' then amount else null end
        ) as amount
        from orders o left join payments p on o.order_id = p.order_id
        group by 1
)

select
    o.order_id,
    o.customer_id,
    o.order_date,
    coalesce(a.amount, 0) as amount
from 
    orders o 
    left join order_amount a using (order_id)
