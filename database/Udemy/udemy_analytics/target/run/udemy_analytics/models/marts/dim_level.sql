
  
    

  create  table "udemy_dw"."public_marts"."dim_level__dbt_tmp"
  
  
    as
  
  (
    SELECT
    ROW_NUMBER() OVER (ORDER BY level) AS level_id,
    level AS level_name
FROM (
    SELECT DISTINCT level 
    FROM "udemy_dw"."public_staging"."stg_courses"
    WHERE level IS NOT NULL
) sub
  );
  