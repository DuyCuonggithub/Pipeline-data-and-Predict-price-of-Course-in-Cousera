
  create view "udemy_dw"."public_staging"."stg_price_tracker__dbt_tmp"
    
    
  as (
    WITH source AS (
    SELECT * FROM "udemy_dw"."raw"."price_tracker"
),

ranked AS (
    SELECT
        course_id,
        TRIM(title) as title,
        COALESCE(original_price, 0) AS list_price,
        COALESCE(discount_price, original_price, 0) AS sale_price,
        _scraped_datetime AS scraped_at,
        _url as course_url,
        -- [FIX] Tạo cột số thứ tự
        ROW_NUMBER() OVER (
            PARTITION BY course_id, _scraped_datetime 
            ORDER BY _scraped_datetime DESC
        ) as rn
    FROM source
)

-- [FIX] Lọc lấy dòng mới nhất
SELECT * FROM ranked WHERE rn = 1
  );