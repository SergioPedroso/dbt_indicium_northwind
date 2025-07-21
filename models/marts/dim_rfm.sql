WITH int_rfm AS(

    SELECT
        customer_id,
        recency_days,
        frequency_orders,
        monetary_value,
        r_score,
        f_score,
        m_score,
        fm_score,
        rfm_segment,
        r_fm_segment

    FROM {{ ref('int_rfm') }}
)

, customers AS (
    
    SELECT
        customer_id,
        customer_name

    FROM {{ ref('stg_customers') }}

)

, rfm AS (

    SELECT
        customers.customer_name,
        int_rfm.recency_days,
        int_rfm.frequency_orders,
        int_rfm.monetary_value,
        int_rfm.r_score,
        int_rfm.f_score,
        int_rfm.m_score,
        int_rfm.fm_score,
        int_rfm.rfm_segment,
        int_rfm.r_fm_segment

    FROM int_rfm
    LEFT JOIN customers USING (customer_id)

)
    
SELECT * FROM rfm