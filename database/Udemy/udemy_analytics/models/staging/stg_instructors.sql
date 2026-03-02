WITH source AS (
    SELECT * FROM {{ source('raw', 'instructors') }}
)

SELECT
    instructor_id,
    TRIM(name) AS instructor_name,
    job_title,
    course_id, 
    num_students AS total_students,
    -- [FIX] Ép kiểu NUMERIC
    ROUND(avg_rating_score::NUMERIC, 1) AS instructor_rating,
    num_of_courses,
    total_num_reviews
    -- [FIX] Tạm bỏ cột _scraped_datetime vì bảng raw instructor có thể không có cột này
    -- Nếu bạn chắc chắn có thì bỏ comment dòng dưới:
    --, _scraped_datetime AS scraped_at 
FROM source