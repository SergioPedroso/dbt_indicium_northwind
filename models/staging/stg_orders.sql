WITH orders AS (
  SELECT * FROM {{ source('northwind', 'orders') }}
)

SELECT
    CAST(order_id AS int) AS order_id,
    CAST(customer_id AS string) AS customer_id,
    CAST(employee_id AS int) AS employee_id,
    DATE(order_date) AS order_date,
    DATE(required_date) AS required_date,
    DATE(shipped_date) AS shipped_date,
    ship_via,
    freight   

FROM orders