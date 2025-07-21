-- Singular test: ensure that order_details quantity values are not <= 0

SELECT
    order_id,
    quantity 
FROM {{ source('northwind', 'order_details') }}
WHERE quantity <= 0 