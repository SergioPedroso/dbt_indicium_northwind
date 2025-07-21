WITH customers AS(

    SELECT
        customer_id,
        customer_name,
        customer_city,
        customer_country

    FROM {{ ref('stg_customers') }}
    
)

SELECT * FROM customers