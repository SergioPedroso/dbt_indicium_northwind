WITH categories AS (
  SELECT * FROM {{ source('northwind', 'categories') }}
)

SELECT     
   
    CAST(category_id AS int) AS category_id,
    category_name
        
FROM categories