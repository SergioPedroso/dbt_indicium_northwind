WITH raw_generate_date AS (

        {{ dbt_date.get_date_dimension("1996-01-01", "1999-01-01") }}
        
)

, generate_date AS (

    SELECT

        raw_generate_date.date_day AS date_day,
        raw_generate_date.month_start_date AS date_start_month,
        raw_generate_date.month_name_short AS month_name_short,
        (raw_generate_date.month_name_short||'/'|| raw_generate_date.year_number ) AS month_year,
        raw_generate_date.year_start_date AS date_start_year,
        raw_generate_date.year_number AS year_number

    FROM raw_generate_date

)

, final_date AS (

    SELECT

        REPLACE(CAST(date_day AS string), '-', '') AS date_id,
        date_day,
        date_start_month,
        month_name_short,
        month_year ,
        date_start_year,
        year_number

    FROM generate_date

)

SELECT * FROM final_date