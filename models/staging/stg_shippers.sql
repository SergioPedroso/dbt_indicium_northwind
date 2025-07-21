WITH shippers AS (
  SELECT * FROM {{ source('northwind', 'shippers') }}
)

SELECT     

    CAST(shipper_id as int) AS shipper_id,
    company_name AS shipper_name
        
FROM shippers