WITH user_page_metrics AS (
    SELECT 
        user_pseudo_id,
        page_location,
        COUNT(*) as page_views,
        COUNT(DISTINCT CONCAT(user_pseudo_id, '-', ga_session_id)) as sessions_with_page,
        SUM(CASE WHEN event_name = 'page_view' THEN 1 ELSE 0 END) as total_page_views,
        AVG(CASE WHEN event_name = 'page_view' THEN 
            CAST(event_params.value.int_value AS FLOAT64) 
            ELSE NULL END) as avg_engagement_time
    FROM `your_project.analytics_xxx.events_*`
    WHERE _TABLE_SUFFIX BETWEEN '20241201' AND '20251231'
    AND event_name = 'page_view'
    GROUP BY user_pseudo_id, page_location
),
user_features AS (
    SELECT 
        user_pseudo_id,
        COUNT(DISTINCT page_location) as unique_pages_visited,
        SUM(page_views) as total_page_views,
        SUM(sessions_with_page) as total_sessions,
        AVG(avg_engagement_time) as avg_engagement_time_per_page,
        -- カテゴリ別ページビュー（URLパターンで分類）
        SUM(CASE WHEN REGEXP_CONTAINS(page_location, r'/jp/ja/company/') THEN page_views ELSE 0 END) as company_page_views,
        SUM(CASE WHEN REGEXP_CONTAINS(page_location, r'/jp/ja/release/') THEN page_views ELSE 0 END) as release_page_views,
        SUM(CASE WHEN REGEXP_CONTAINS(page_location, r'/jp/ja/sustainability/') THEN page_views ELSE 0 END) as sustainability_page_views,
        SUM(CASE WHEN REGEXP_CONTAINS(page_location, r'/jp/ja/ir/') THEN page_views ELSE 0 END) as ir_page_views,
        SUM(CASE WHEN REGEXP_CONTAINS(page_location, r'/jp/ja/careers/') THEN page_views ELSE 0 END) as careers_page_views
    FROM user_page_metrics
    GROUP BY user_pseudo_id
)
SELECT * FROM user_features;


-- BigQuery MLモデルの作成
-- ユーザーベースのクラスタリングモデルを作成
CREATE OR REPLACE MODEL `your_project.your_dataset.user_clustering_model`
OPTIONS(
    model_type='KMEANS',
    num_clusters=5,
    standardize_features=TRUE
) AS
SELECT
    unique_pages_visited,
    total_page_views,
    total_sessions,
    avg_engagement_time_per_page,
    company_page_views / total_page_views as company_ratio,
    release_page_views / total_page_views as release_ratio,
    sustainability_page_views / total_page_views as sustainability_ratio,
    ir_page_views / total_page_views as ir_ratio,
    careers_page_views / total_page_views as careers_ratio
FROM user_features
WHERE total_page_views > 5  -- ノイズ除去
