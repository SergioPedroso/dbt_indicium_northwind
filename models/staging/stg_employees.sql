WITH employees AS (
  SELECT * FROM {{ source('northwind', 'employees') }}
)

SELECT     
   
    CAST(employee_id AS int) AS employee_id,
    CONCAT(title_of_courtesy,' ',first_name,' ',last_name) AS employee_name,
    title AS employee_title,
    DATE(hire_date) AS hire_date,
    DATE_DIFF(hire_date, '1998-08-01', MONTH) * -1 AS months_working,
    city AS employee_city,
    country AS employee_country,
        
FROM employees