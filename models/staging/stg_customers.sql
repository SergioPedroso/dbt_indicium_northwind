WITH customers AS (
  SELECT * FROM {{ source('northwind', 'customers') }}
)

SELECT     
   
    CAST(customer_id AS string) AS customer_id,
    company_name AS customer_name,
    city AS customer_city,    
    country AS customer_country
        
FROM customers