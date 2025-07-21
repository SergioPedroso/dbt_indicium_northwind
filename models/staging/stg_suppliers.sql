WITH suppliers AS (
  SELECT * FROM {{ source('northwind', 'suppliers') }}
)

SELECT     

    CAST(supplier_id AS int) AS supplier_id,
    company_name AS supplier_name,
    city AS supplier_city,
    country AS supplier_country
        
FROM suppliers