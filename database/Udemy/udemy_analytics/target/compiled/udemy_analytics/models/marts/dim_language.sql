SELECT
    ROW_NUMBER() OVER (ORDER BY language) AS language_id,
    language AS language_name
FROM (
    SELECT DISTINCT language 
    FROM "udemy_dw"."public_staging"."stg_courses"
    WHERE language IS NOT NULL
) sub