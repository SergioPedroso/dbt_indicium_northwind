WITH products AS (
  SELECT * FROM {{ source('northwind', 'products') }}
)

SELECT     
   
    CAST(product_id AS int) AS product_id,   
    product_name,
    CAST(supplier_id AS int) AS supplier_id,
    CAST(category_id AS int) AS category_id,
    unit_price,
    discontinued AS is_discontinued       
        
FROM products