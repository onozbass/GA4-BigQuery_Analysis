SELECT 
    -- セッション識別子
    user_pseudo_id,
    ga_session_id,
    CONCAT(user_pseudo_id, '-', ga_session_id) AS session_id,
--    CONCAT(user_pseudo_id, '-', CAST(ga_session_id AS STRING)) AS session_id,

    -- 日時情報
    PARSE_DATE('%Y/%m/%d', event_date) AS session_date,
    TIMESTAMP_MICROS(MIN(event_timestamp)) AS session_start_time,
    TIMESTAMP_MICROS(MAX(event_timestamp)) AS session_start_time,
--    MIN(TIMESTAMP_MICROS(event_timestamp)) AS session_start_time,
--    MAX(TIMESTAMP_MICROS(event_timestamp)) AS session_end_time,
    TIMESTAMP_DIFF(
        TIMESTAMP_MICROS(MAX(event_timestamp)), 
        TIMESTAMP_MICROS(MIN(event_timestamp)), 
--        MAX(TIMESTAMP_MICROS(event_timestamp)), 
--        MIN(TIMESTAMP_MICROS(event_timestamp)), 
        SECOND
    ) AS session_duration_seconds,

    -- セッション基本メトリクス
    COUNT(*) AS total_events,
    COUNTIF(event_name = 'page_view') AS page_views,
    COUNT(DISTINCT CASE WHEN event_name = 'page_view' THEN CONCAT(
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location'),
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_title')
    ) END) AS unique_pages,

    -- エンゲージメント
    COUNTIF(event_name = 'scroll') AS scroll_events,
    COUNTIF(event_name = 'click') AS click_events,
    COUNTIF(event_name = 'file_download') AS file_downloads,

    -- セッション品質指標
    MAX((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'engaged_session_event')) AS is_engaged_session,
    MAX((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'session_engaged')) AS session_engaged,

    -- 地理情報（セッション開始時点）
    ARRAY_AGG(geo.continet ORDER BY event_timestamp ASC LIMIT 1)[OFFSET(0)] AS continent,
    ARRAY_AGG(geo.sub_continent ORDER BY event_timestamp ASC LIMIT 1)[OFFSET(0)] AS sub_continent,
    ARRAY_AGG(geo.country ORDER BY event_timestamp ASC LIMIT 1)[OFFSET(0)] AS country,
    ARRAY_AGG(geo.region ORDER BY event_timestamp ASC LIMIT 1)[OFFSET(0)] AS region,
    ARRAY_AGG(geo.city ORDER BY event_timestamp ASC LIMIT 1)[OFFSET(0)] AS city,

    -- デバイス情報（セッション開始時点）
    ARRAY_AGG(device.category ORDER BY event_timestamp ASC LIMIT 1)[OFFSET(0)] AS device_category,
    ARRAY_AGG(device.operating_system ORDER BY event_timestamp ASC LIMIT 1)[OFFSET(0)] AS operating_system,
    ARRAY_AGG(device.web_info.browser ORDER BY event_timestamp ASC LIMIT 1)[OFFSET(0)] AS browser,
    ARRAY_AGG(device.language ORDER BY event_timestamp ASC LIMIT 1)[OFFSET(0)] AS language,

    -- 流入元情報（セッション開始時点）
    ARRAY_AGG(traffic_source.source ORDER BY event_timestamp ASC LIMIT 1)[OFFSET(0)] AS traffic_source,
    ARRAY_AGG(traffic_source.medium ORDER BY event_timestamp ASC LIMIT 1)[OFFSET(0)] AS traffic_medium,
    ARRAY_AGG(traffic_source.name ORDER BY event_timestamp ASC LIMIT 1)[OFFSET(0)] AS campaign_name,
    
    ARRAY_AGG(
      IF(event_name = 'session_start',
         CONCAT(COALESCE(session_source, '(direct)'), ' / ', COALESCE(session_medium, '(none)')),
         NULL
      ) IGNORE NULLS
      ORDER BY event_timestamp DESC LIMIT 1
    )[OFFSET(0)] AS traffic_source2,

    -- ランディングページ
    ARRAY_AGG(
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') ORDER BY event_timestamp ASC LIMIT 1
    )[OFFSET(0)] AS landing_page,

    -- 離脱ページ
    ARRAY_AGG(
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') ORDER BY event_timestamp DESC LIMIT 1
    )[OFFSET(0)] AS exit_page

FROM `your-project.your_dataset.events_*`
WHERE _TABLE_SUFFIX BETWEEN '20241201' AND '20251231'
    AND ga_session_id IS NOT NULL
GROUP BY user_pseudo_id, ga_session_id, session_date
ORDER BY session_start_time ASC
