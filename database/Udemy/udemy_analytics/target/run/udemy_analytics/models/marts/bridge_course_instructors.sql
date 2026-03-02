
  
    

  create  table "udemy_dw"."public_marts"."bridge_course_instructors__dbt_tmp"
  
  
    as
  
  (
    SELECT DISTINCT
    course_id,
    instructor_id
FROM "udemy_dw"."public_staging"."stg_instructors"
  );
  