SELECT
    ROW_NUMBER() OVER (ORDER BY level) AS level_id,
    level AS level_name
FROM (
    SELECT DISTINCT level 
    FROM {{ ref('stg_courses') }}
    WHERE level IS NOT NULL
) sub