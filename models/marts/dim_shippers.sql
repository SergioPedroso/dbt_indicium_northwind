WITH shippers AS(
    SELECT * FROM {{ ref('stg_shippers') }}
)

SELECT

    shipper_id,
    shipper_name

FROM shippers