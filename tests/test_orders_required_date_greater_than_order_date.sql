-- Singular test: ensure that required date is greater than order date

SELECT
    order_id,
    order_date,
    required_date
FROM {{ source('northwind', 'orders') }}
WHERE order_date > required_date

