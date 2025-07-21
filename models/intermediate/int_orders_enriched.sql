WITH orders_details AS (
  SELECT

      order_id,
      product_id,
      quantity,
      unit_price,
      discount,
      quantity * unit_price * (1 - discount) AS revenue

  FROM {{ ref('stg_order_details') }}
)

, orders AS (
    SELECT

        order_id,
        customer_id,
        employee_id,
        order_date,
        REPLACE(CAST(order_date AS string), '-', '') AS date_id,
        freight

    FROM {{ ref('stg_orders') }}
)

, orders_enriched AS (

    SELECT

        orders_details.order_id,
        orders_details.product_id,
        orders_details.quantity,
        orders_details.unit_price,
        orders_details.discount,
        orders_details.revenue,
        orders.customer_id,
        orders.employee_id,
        orders.order_date,
        orders.date_id,
        orders.freight

    FROM orders_details
    LEFT JOIN orders USING (order_id)

)

SELECT * FROM orders_enriched





