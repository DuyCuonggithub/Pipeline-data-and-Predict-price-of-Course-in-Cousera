{{ config(materialized='table') }}

WITH date_boundaries AS (
    SELECT
        MIN(LEAST(
            published_date, 
            last_updated_date, 
            scraped_at::DATE
        )) AS start_date,

        MAX(GREATEST(
            published_date, 
            last_updated_date, 
            scraped_at::DATE
        )) AS end_date
    FROM {{ ref('stg_courses') }}
),

date_sequence AS (
    SELECT
        (generate_series(
            start_date::TIMESTAMP, 
            end_date::TIMESTAMP, 
            '1 day'::INTERVAL -- [FIXED] Sửa NTERVAL thành INTERVAL
        ))::DATE AS date_day
    FROM date_boundaries
)

SELECT
    date_day AS date_key,
    EXTRACT(YEAR FROM date_day) AS year,
    EXTRACT(MONTH FROM date_day) AS month,
    EXTRACT(DAY FROM date_day) AS day,
    EXTRACT(QUARTER FROM date_day) AS quarter,
    EXTRACT(ISODOW FROM date_day) AS day_of_week,
    TO_CHAR(date_day, 'Month') AS month_name,
    TO_CHAR(date_day, 'Day') AS day_name,
    CASE 
        WHEN EXTRACT(ISODOW FROM date_day) IN (6, 7) THEN true 
        ELSE false 
    END AS is_weekend,
    TO_CHAR(date_day, 'YYYY-MM') AS year_month
FROM date_sequence