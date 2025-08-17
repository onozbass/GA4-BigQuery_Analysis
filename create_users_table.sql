SELECT DISTINCT
    user_pseudo_id,
    first_visit_date,
    MIN(PARSE_DATE('%Y/%m/%d', event_date)) AS first_seen_date,
    MAX(PARSE_DATE('%Y/%m/%d', event_date)) AS last_seen_date,
    COUNT(DISTINCT PARSE_DATE('%Y/%m/%d', event_date)) AS active_days,

    -- セッション数とイベント数
    COUNT(DISTINCT CONCAT(user_pseudo_id, '-', ga_session_id)) AS total_sessions,
    COUNT(*) AS total_events,

    -- 地理情報（最新の情報を取得）
    ARRAY_AGG(geo.continent ORDER BY event_timestamp DESC LIMIT 1)[OFFSET(0)] AS last_continent,
    ARRAY_AGG(geo.sub_continent ORDER BY event_timestamp DESC LIMIT 1)[OFFSET(0)] AS last_sub_continent,
    ARRAY_AGG(geo.country ORDER BY event_timestamp DESC LIMIT 1)[OFFSET(0)] AS last_country,
    ARRAY_AGG(geo.region ORDER BY event_timestamp DESC LIMIT 1)[OFFSET(0)] AS last_region,
    ARRAY_AGG(geo.city ORDER BY event_timestamp DESC LIMIT 1)[OFFSET(0)] AS last_city,

    -- デバイス情報（最新の情報を取得）
    ARRAY_AGG(device.category ORDER BY event_timestamp DESC LIMIT 1)[OFFSET(0)] AS last_device_category,
    ARRAY_AGG(device.operating_system ORDER BY event_timestamp DESC LIMIT 1)[OFFSET(0)] AS last_operating_system,
    ARRAY_AGG(device.web_info.browser ORDER BY event_timestamp DESC LIMIT 1)[OFFSET(0)] AS last_browser,
    ARRAY_AGG(device.language ORDER BY event_timestamp DESC LIMIT 1)[OFFSET(0)] AS last_language,

    -- トラフィック情報（最初の流入元）
    ARRAY_AGG(traffic_source.source ORDER BY event_timestamp ASC LIMIT 1)[OFFSET(0)] AS first_traffic_source,
    ARRAY_AGG(traffic_source.medium ORDER BY event_timestamp ASC LIMIT 1)[OFFSET(0)] AS first_traffic_medium,
    ARRAY_AGG(traffic_source.name ORDER BY event_timestamp ASC LIMIT 1)[OFFSET(0)] AS first_campaign_name

FROM `your-project.your_dataset.events_*`
WHERE _TABLE_SUFFIX BETWEEN '202401201' AND '20251231'
    AND user_pseudo_id IS NOT NULL
GROUP BY user_pseudo_id
ORDER BY first_visit_date ASC
