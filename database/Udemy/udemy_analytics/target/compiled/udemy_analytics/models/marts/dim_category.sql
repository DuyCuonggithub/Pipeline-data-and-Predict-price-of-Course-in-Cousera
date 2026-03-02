SELECT
    -- Đánh số tự tăng: 1, 2, 3... dựa theo tên A->Z
    ROW_NUMBER() OVER (ORDER BY category) AS category_id,
    category AS category_name
FROM (
    SELECT DISTINCT category 
    FROM "udemy_dw"."public_staging"."stg_courses"
    WHERE category IS NOT NULL
) sub