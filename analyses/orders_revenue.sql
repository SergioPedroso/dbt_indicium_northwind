WITH orders AS (
    SELECT
        order_date AS date_day,
        revenue

    FROM {{ ref('fact_orders') }}

)

, dates AS (
    SELECT
        date_day,
        month_year

    FROM {{ ref('dim_dates') }}

)

, monthly_revenue AS (

    SELECT
        month_year,
        SUM(revenue)

    FROM orders
    LEFT JOIN dates USING (date_day)
    GROUP BY month_year

)

SELECT * FROM monthly_revenue