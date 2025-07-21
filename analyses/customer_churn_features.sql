{{ config(
    materialized = 'table',
) }}

WITH orders AS (

    SELECT
        order_id,
        customer_id,
        order_date

    FROM {{ ref('stg_orders') }}

)

, order_details AS (

    SELECT
        product_id,
        order_id,
        CAST(quantity AS INT64) AS qntd,
        CAST(unit_price AS NUMERIC) AS unit_price,
        CAST(discount AS NUMERIC) AS discount_pct,
        CAST(unit_price * quantity * (1 - discount) AS NUMERIC) AS revenue

    FROM {{ ref('stg_order_details') }}
)

, products AS (

    SELECT
        product_id,
        product_name,
        unit_price,
        is_discontinued,
        category_name,
        supplier_name,
        supplier_city,
        supplier_country

    FROM {{ ref('dim_products') }}

)

, orders_enriched AS (

    SELECT
        orders.order_id,
        orders.customer_id,
        orders.order_date,
        products.product_name,
        products.category_name,
        products.supplier_name,
        products.supplier_city,
        products.supplier_country,
        order_details.qntd,
        order_details.unit_price,
        order_details.discount_pct,
        order_details.revenue

    FROM orders
    LEFT JOIN order_details USING (order_id)
    LEFT JOIN products USING (product_id)

)

, churn AS (
    SELECT
        customer_id,
        is_churn

    FROM {{ ref('dim_customer_churn') }}
)

, customer_metrics AS (

    SELECT
        customer_id,
        COUNT(DISTINCT order_id) AS total_orders,
        COUNT(*) AS total_items,
        SUM(revenue) AS total_revenue,
        AVG(revenue) AS avg_order_value,
        AVG(qntd) AS avg_qntd_per_item,
        AVG(discount_pct) AS avg_discount_pct,
        SUM(CASE 
                WHEN discount_pct > 0 THEN 1 
                ELSE 0 
            END)/COUNT(*) AS pct_items_discounted,
        COUNT(DISTINCT category_name) AS distinct_categories,
        MIN(order_date) AS first_order_date,
        MAX(order_date) AS last_order_date,
        DATE_DIFF(CURRENT_DATE(), MAX(order_date), DAY) AS recency_days,
        DATE_DIFF(MAX(order_date), MIN(order_date), DAY) AS tenure_days,
        MAX(churn.is_churn) AS is_churn
    FROM orders_enriched
    LEFT JOIN churn USING(customer_id)
    GROUP BY customer_id
)

SELECT * FROM customer_metrics
