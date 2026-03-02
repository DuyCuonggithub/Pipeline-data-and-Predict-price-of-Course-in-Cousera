
  
    

  create  table "udemy_dw"."public_marts"."dim_instructor__dbt_tmp"
  
  
    as
  
  (
    WITH ranked AS (
    SELECT
        instructor_id,
        instructor_name,
        job_title,
        -- Ưu tiên lấy thông tin từ lần cào có tổng học viên cao nhất (dữ liệu đầy đủ nhất)
        ROW_NUMBER() OVER (PARTITION BY instructor_id ORDER BY total_students DESC) as rn
    FROM "udemy_dw"."public_staging"."stg_instructors"
)

SELECT
    instructor_id,
    instructor_name,
    job_title
FROM ranked
WHERE rn = 1
  );
  