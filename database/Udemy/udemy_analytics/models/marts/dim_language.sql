SELECT
    ROW_NUMBER() OVER (ORDER BY language) AS language_id,
    language AS language_name
FROM (
    SELECT DISTINCT language 
    FROM {{ ref('stg_courses') }}
    WHERE language IS NOT NULL
) sub