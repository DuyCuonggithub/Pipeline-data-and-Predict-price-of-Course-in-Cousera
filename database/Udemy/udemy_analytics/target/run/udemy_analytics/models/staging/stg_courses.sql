
  create view "udemy_dw"."public_staging"."stg_courses__dbt_tmp"
    
    
  as (
    WITH source AS (
    SELECT * FROM "udemy_dw"."raw"."courses"
),

ranked AS (
    SELECT
        course_id,
        TRIM(title) AS title,
        TRIM(headline) AS headline,
        language,
        level,
        -- [FIX] Ép kiểu NUMERIC trước khi ROUND
        ROUND((course_duration_seconds / 3600.0)::NUMERIC, 1) AS duration_hours,
        publishes_date::DATE AS published_date,
        lasted_updated_date::DATE AS last_updated_date,
        COALESCE(original_price, 0) AS list_price,
        COALESCE(discount_price, original_price, 0) AS sale_price,
        num_students,
        num_reviews,
        -- [FIX] Ép kiểu NUMERIC trước khi ROUND
        ROUND(avg_rating_score::NUMERIC, 1) AS rating,
        rating_distribution,
        _url AS course_url,
        _category AS category,
        _scraped_datetime AS scraped_at,
        
        -- [FIX] Tạo cột số thứ tự để thay thế QUALIFY
        ROW_NUMBER() OVER (
            PARTITION BY course_id, _scraped_datetime 
            ORDER BY _scraped_datetime DESC
        ) as rn
    FROM source
),

cleaned AS (
    -- [FIX] Lọc lấy dòng đầu tiên
    SELECT * FROM ranked WHERE rn = 1
),

ratings AS (
    SELECT
        -- Liệt kê các cột cần lấy (bỏ cột rn đi)
        course_id, title, headline, language, level, duration_hours,
        published_date, last_updated_date, list_price, sale_price,
        num_students, num_reviews, rating, course_url, category, scraped_at,
        rating_distribution,

        -- Parse JSON (Cú pháp này của Postgres OK)
        CAST(rating_distribution::json->0->>'count' AS INT) AS rating_1,
        CAST(rating_distribution::json->1->>'count' AS INT) AS rating_2,
        CAST(rating_distribution::json->2->>'count' AS INT) AS rating_3,
        CAST(rating_distribution::json->3->>'count' AS INT) AS rating_4,
        CAST(rating_distribution::json->4->>'count' AS INT) AS rating_5
    FROM cleaned
)

SELECT * FROM ratings
  );