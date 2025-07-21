WITH fact_orders AS(

    SELECT
        order_id,
        product_id,
        quantity,
        unit_price,
        discount,
        revenue,
        customer_id,
        employee_id,
        date_id,
        order_date,
        freight

    FROM {{ ref('int_orders_enriched') }}

)


SELECT * FROM fact_orders