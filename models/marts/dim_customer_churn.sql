WITH dim_customer_churn AS (
    
    SELECT
        customer_id,
        customer_name,
        last_order_date,
        months_since_last_order,
        is_churn

    FROM {{ ref('int_customer_last_purchase') }}

)
    
SELECT * FROM dim_customer_churn