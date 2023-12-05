select
    id as payment_id,
    orderid as order_id,
    paymentmethod as method,
    status,
    amount,
    created as created_at
from 
    {{ source('stripe', 'payment') }}