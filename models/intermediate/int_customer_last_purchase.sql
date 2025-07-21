WITH orders AS (

    SELECT
        customer_id,
        order_date

    FROM {{ ref('stg_orders') }}

)

, ref_date AS (

    SELECT
        MAX(order_date) AS max_date

    FROM orders
)


, last_purchase AS (

    SELECT
        customer_id,
        MAX(order_date) AS last_order_date

    FROM orders
    GROUP BY customer_id
)

, churn_flag AS (

    SELECT
        last_purchase.customer_id,
        last_purchase.last_order_date,
        DATE_DIFF(ref_date.max_date, last_purchase.last_order_date, MONTH) AS months_since_last_order,
        CASE
            WHEN DATE_DIFF(ref_date.max_date, last_purchase.last_order_date, MONTH) > 4 THEN True
            ELSE False
        END AS is_churn

    FROM last_purchase
    CROSS JOIN ref_date
)

, customers_churn AS (

    SELECT
        customers.customer_id,
        customers.customer_name,
        churn_flag.last_order_date,
        churn_flag.months_since_last_order,
        churn_flag.is_churn
    FROM {{ ref('stg_customers') }} customers
    LEFT JOIN churn_flag USING (customer_id)
    WHERE churn_flag.is_churn IS NOT NULL
    ORDER BY months_since_last_order DESC

)

SELECT * FROM customers_churn
