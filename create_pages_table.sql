WITH page_events AS (
  SELECT 
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS page_location,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_title') AS page_title,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_referrer') AS page_referrer,
    event_timestamp,
    user_pseudo_id,
    event_date
  FROM `your-project.your_dataset.events_*`
  WHERE _TABLE_SUFFIX BETWEEN '20241201' AND '20251231'
    AND event_name = 'page_view'
    AND (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') IS NOT NULL
),
page_analysis AS (
  SELECT 
    page_location,
    page_title,
    page_referrer,
    
    -- FQDN（Fully Qualified Domain Name）の抽出
    CASE 
      WHEN REGEXP_CONTAINS(page_location, r'^https?://') THEN
        REGEXP_EXTRACT(page_location, r'^https?://([^/?\s]+)')
      ELSE NULL
    END AS fqdn,
    
    -- クエリパラメータ部分の抽出（?以降）
    CASE 
      WHEN REGEXP_CONTAINS(page_location, r'\?') THEN
        REGEXP_EXTRACT(page_location, r'\?(.*)$')
      ELSE NULL
    END AS query_parameters,
    
    -- パス部分の抽出（FQDNとクエリパラメータを除く）
    CASE 
      WHEN REGEXP_CONTAINS(page_location, r'^https?://') THEN
        CASE 
          WHEN REGEXP_CONTAINS(page_location, r'\?') THEN
            REGEXP_EXTRACT(page_location, r'^https?://[^/?\s]+(\/[^?\s]*)')
          ELSE
            REGEXP_EXTRACT(page_location, r'^https?://[^/?\s]+(\/.*)')
        END
      ELSE page_location  -- プロトコルがない場合はそのまま
    END AS page_path,
    
    -- 統計情報の計算
    COUNT(*) AS total_page_views,
    COUNT(DISTINCT user_pseudo_id) AS unique_users,
    MIN(PARSE_DATE('%Y%m%d', event_date)) AS first_seen_date,
    MAX(PARSE_DATE('%Y%m%d', event_date)) AS last_seen_date,
    COUNT(DISTINCT event_date) AS active_days
    
  FROM page_events
  GROUP BY page_location, page_title, page_referrer
)



SELECT 
  -- 基本ページ情報
  page_location,
  page_title,
  page_referrer,
  
  -- URL分析結果
  fqdn,
  query_parameters,
  page_path,
  
  -- パス階層分析（最大5階層まで）
  CASE 
    WHEN page_path IS NOT NULL THEN
      SPLIT(TRIM(page_path, '/'), '/')[SAFE_OFFSET(0)]
    ELSE NULL
  END AS path_level_1,
  
  CASE 
    WHEN page_path IS NOT NULL THEN
      SPLIT(TRIM(page_path, '/'), '/')[SAFE_OFFSET(1)]
    ELSE NULL
  END AS path_level_2,
  
  CASE 
    WHEN page_path IS NOT NULL THEN
      SPLIT(TRIM(page_path, '/'), '/')[SAFE_OFFSET(2)]
    ELSE NULL
  END AS path_level_3,
  
  -- パスの深さ
  CASE 
    WHEN page_path IS NOT NULL THEN
      ARRAY_LENGTH(SPLIT(TRIM(page_path, '/'), '/'))
    ELSE 0
  END AS path_depth,
  
  -- クエリパラメータの分析
  CASE 
    WHEN query_parameters IS NOT NULL THEN
      ARRAY_LENGTH(SPLIT(query_parameters, '&'))
    ELSE 0
  END AS query_param_count,
  
  -- ページタイプの分類
  CASE 
    WHEN page_path = '/' OR page_path IS NULL THEN 'Homepage'
    WHEN REGEXP_CONTAINS(LOWER(IFNULL(page_path, '')), r'/(product|item|p)s?/') THEN 'Product Page'
    WHEN REGEXP_CONTAINS(LOWER(IFNULL(page_path, '')), r'/(category|c)/') THEN 'Category Page'
    WHEN REGEXP_CONTAINS(LOWER(IFNULL(page_path, '')), r'/(blog|article|news)/') THEN 'Content Page'
    WHEN REGEXP_CONTAINS(LOWER(IFNULL(page_path, '')), r'/(cart|checkout|order)') THEN 'Commerce Page'
    WHEN REGEXP_CONTAINS(LOWER(IFNULL(page_path, '')), r'/(contact|about|help|support)') THEN 'Info Page'
    WHEN REGEXP_CONTAINS(LOWER(IFNULL(page_path, '')), r'/(search|results)') THEN 'Search Page'
    ELSE 'Other'
  END AS page_type,
  
  -- 統計情報
  total_page_views,
  unique_users,
  ROUND(total_page_views / unique_users, 2) AS avg_views_per_user,
  first_seen_date,
  last_seen_date,
  active_days,
  
  -- ページパフォーマンス指標
  CASE 
    WHEN total_page_views >= 1000 THEN 'High Traffic'
    WHEN total_page_views >= 100 THEN 'Medium Traffic'
    WHEN total_page_views >= 10 THEN 'Low Traffic'
    ELSE 'Very Low Traffic'
  END AS traffic_level,
  
  -- URL品質チェック
  CASE 
    WHEN fqdn IS NULL THEN 'Invalid URL'
    WHEN REGEXP_CONTAINS(page_location, r'[A-Z]') THEN 'Contains Uppercase'
    WHEN LENGTH(page_location) > 255 THEN 'Too Long'
    WHEN REGEXP_CONTAINS(page_location, r'[^a-zA-Z0-9\-._~:/?#\[\]@!$&''()*+,;=%]') THEN 'Special Characters'
    ELSE 'Clean URL'
  END AS url_quality,
  
  -- SEOフレンドリー度チェック
  CASE 
    WHEN page_title IS NULL OR LENGTH(TRIM(page_title)) = 0 THEN 'No Title'
    WHEN LENGTH(page_title) < 30 THEN 'Title Too Short'
    WHEN LENGTH(page_title) > 60 THEN 'Title Too Long'
    ELSE 'Title OK'
  END AS title_quality

FROM page_analysis
WHERE page_location IS NOT NULL
ORDER BY page_location ASC
