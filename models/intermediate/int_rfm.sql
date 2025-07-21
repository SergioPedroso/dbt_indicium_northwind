WITH orders AS (

    SELECT
        order_id,
        order_date,
        customer_id,

    FROM {{ ref('stg_orders') }}

)

, orders_details AS (
    
    SELECT
        order_id,
        quantity * unit_price * (1 - discount) AS revenue

    FROM {{ ref('stg_order_details') }}

)

, orders_enriched AS (

    SELECT
        orders.order_id,
        orders.customer_id,
        SUM(orders_details.revenue) AS order_amount, 
        orders.order_date

    FROM orders
    LEFT JOIN orders_details USING (order_id)
    GROUP BY order_id, customer_id, order_date
)

, rfm_base AS (

    SELECT
        customer_id,
        DATE_DIFF(MAX(order_date), '1998-08-01', DAY) * -1 AS recency_days,   -- (Recency) days since last order
        COUNT(order_id) AS frequency_orders, -- (Frequency) orders volume
        SUM(order_amount) AS monetary_value -- (Value) value spent

    FROM orders_enriched
    GROUP BY customer_id

)

, quantiles AS (

    SELECT
        APPROX_QUANTILES(recency_days, 3)[OFFSET(0)] AS r_q1,
        APPROX_QUANTILES(recency_days, 3)[OFFSET(1)] AS r_q2,
        APPROX_QUANTILES(recency_days, 3)[OFFSET(2)] AS r_q3,
        APPROX_QUANTILES(recency_days, 3)[OFFSET(3)] AS r_q4,
        APPROX_QUANTILES(frequency_orders, 3)[OFFSET(0)] AS f_q1,
        APPROX_QUANTILES(frequency_orders, 3)[OFFSET(1)] AS f_q2,
        APPROX_QUANTILES(frequency_orders, 3)[OFFSET(2)] AS f_q3,
        APPROX_QUANTILES(frequency_orders, 3)[OFFSET(3)] AS f_q4,
        APPROX_QUANTILES(monetary_value, 3)[OFFSET(0)] AS m_q1,
        APPROX_QUANTILES(monetary_value, 3)[OFFSET(1)] AS m_q2,
        APPROX_QUANTILES(monetary_value, 3)[OFFSET(2)] AS m_q3,
        APPROX_QUANTILES(monetary_value, 3)[OFFSET(3)] AS m_q4

    FROM rfm_base

)

, ranking AS (
    SELECT
        rfm_base.*,
        -- Recency
        CASE
            WHEN rfm_base.recency_days <= quantiles.r_q1 THEN 5
            WHEN rfm_base.recency_days <= quantiles.r_q2 THEN 4
            WHEN rfm_base.recency_days <= quantiles.r_q3 THEN 3
            WHEN rfm_base.recency_days <= quantiles.r_q4 THEN 2
            ELSE 1
        END AS r_score,

        -- Frequency
        CASE
            WHEN rfm_base.frequency_orders >= quantiles.f_q4 THEN 5
            WHEN rfm_base.frequency_orders >= quantiles.f_q3 THEN 4
            WHEN rfm_base.frequency_orders >= quantiles.f_q2 THEN 3
            WHEN rfm_base.frequency_orders >= quantiles.f_q1 THEN 2
            ELSE 1
        END AS f_score,

        -- Monetary
        CASE
            WHEN rfm_base.monetary_value >= quantiles.m_q4 THEN 5
            WHEN rfm_base.monetary_value >= quantiles.m_q3 THEN 4
            WHEN rfm_base.monetary_value >= quantiles.m_q2 THEN 3
            WHEN rfm_base.monetary_value >= quantiles.m_q1 THEN 2
            ELSE 1
        END AS m_score
    FROM rfm_base
    CROSS JOIN quantiles
)

, rfm_scores AS (

    SELECT
        customer_id,
        recency_days,
        frequency_orders,
        ROUND(monetary_value,2) AS monetary_value,
        r_score,
        f_score,
        m_score,
        (f_score + m_score)/2 AS fm_score

    FROM ranking

)

SELECT 
    *,
    CASE
        WHEN r_score >= 3
         AND f_score >= 4
         AND m_score >= 4
        THEN 'Campeões'

        WHEN r_score <= 2
         AND f_score >= 4
         AND m_score >= 4
        THEN 'Não posso perdê-los'

        WHEN r_score <= 2
         AND f_score BETWEEN 2 AND 3
         AND m_score BETWEEN 2 AND 3
        THEN 'Em risco'

        WHEN r_score >= 4
         AND f_score BETWEEN 3 AND 4
         AND m_score BETWEEN 3 AND 4
        THEN 'Leais'

        WHEN r_score = 5
         AND f_score <= 2
         AND m_score <= 2
        THEN 'Promissores'

        WHEN r_score BETWEEN 3 AND 4
         AND f_score BETWEEN 2 AND 3
         AND m_score BETWEEN 2 AND 3
        THEN 'Precisam de Atenção'

        WHEN r_score <= 2
         AND f_score <= 2
         AND m_score <= 2
        THEN 'Perdidos'

        ELSE 'Outros'
    END AS rfm_segment,

    CASE
        WHEN r_score >= 3
         AND fm_score >= 4
        THEN 'Campeões'

        WHEN r_score <= 2
         AND fm_score >= 4
        THEN 'Não posso perdê-los'

        WHEN r_score <= 2
         AND fm_score BETWEEN 2 AND 3
        THEN 'Em risco'

        WHEN r_score >= 4
         AND fm_score >= 3
        THEN 'Leais'

        WHEN r_score = 5
         AND fm_score <= 2
        THEN 'Promissores'

        WHEN r_score BETWEEN 3 AND 4
         AND fm_score BETWEEN 2 AND 3
        THEN 'Precisam de Atenção'

        WHEN r_score <= 2
         AND fm_score <= 2
        THEN 'Perdidos'

        ELSE 'Outros'
    END AS r_fm_segment

FROM rfm_scores


