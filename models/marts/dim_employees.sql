WITH employees AS(

    SELECT
        employee_id,
        employee_name,
        employee_title,
        hire_date,
        months_working,
        employee_city,
        employee_country

    FROM {{ ref('stg_employees') }}
)
    
SELECT * FROM employees

        