WITH order_details AS (
  SELECT * FROM {{ source('northwind', 'order_details') }}
)

SELECT
    CAST(order_id AS int) AS order_id,           
    CAST(product_id AS int) AS product_id,             
    unit_price,           
    quantity,            
    discount          

FROM order_details