SELECT DISTINCT
    course_id,
    instructor_id
FROM {{ ref('stg_instructors') }}