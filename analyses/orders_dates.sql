SELECT
    MIN(order_date) AS first_date,
    MAX(order_date) AS last_date

FROM {{ ref('fact_orders') }}