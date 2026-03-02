WITH source AS (
    SELECT * FROM {{ ref('stg_price_tracker') }}
)

SELECT
    -- Tạo ID duy nhất
    MD5(CAST(course_id AS TEXT) || '-' || CAST(scraped_at AS TEXT)) AS history_id,
    
    course_id,
    -- [MỚI] Thêm Title và URL từ chính bảng Tracker (Luôn có dữ liệu)
    title,
    course_url,
    
    list_price,
    sale_price,
    scraped_at AS recorded_at,
    
    -- Tính % giảm giá
    CASE 
        WHEN list_price > 0 THEN ROUND((1 - (sale_price / list_price))::NUMERIC * 100, 0)
        ELSE 0 
    END AS discount_percentage,
    
    -- Cờ báo đáy lịch sử
    CASE 
        WHEN sale_price <= MIN(sale_price) OVER (PARTITION BY course_id) THEN TRUE 
        ELSE FALSE 
    END AS is_lowest_price_ever

FROM source