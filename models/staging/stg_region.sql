WITH region AS (
    SELECT * FROM {{ source('northwind', 'region') }}
)

SELECT 

    CAST(region_id AS int) AS region_id,
    region_description
    
FROM region
