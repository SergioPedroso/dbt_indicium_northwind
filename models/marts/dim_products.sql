WITH products AS (

    SELECT * FROM {{ ref('stg_products') }}

)

, categories AS (

    SELECT * FROM {{ ref('stg_categories') }}

)

, suppliers AS (

    SELECT * FROM {{ ref('stg_suppliers') }}
     
)


, dim_products AS(
    SELECT

        products.product_id,
        products.product_name,
        products.unit_price,
        products.is_discontinued,
        categories.category_name,
        suppliers.supplier_name,
        suppliers.supplier_city,
        suppliers.supplier_country

    FROM products
    LEFT JOIN categories USING (category_id)
    LEFT JOIN suppliers USING (supplier_id)

)


SELECT * FROM dim_products