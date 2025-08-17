SELECT
    FORMAT_TIMESTAMP('%Y/%m/%d %H:%M:%E6S', TIMESTAMP_MICROS(event_timestamp)) AS event_timestamp_utc,
    FORMAT_TIMESTAMP('%Y/%m/%d %H:%M:%E6S', TIMESTAMP_MICROS(event_timestamp), 'Asia/Tokyo') AS event_timestamp_jst,
    event_date, -- ★日本時間基準に変更が必要？GA4プロパティのタイムゾーンに依存しているらしい
    event_name,
    user_pseudo_id,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS ga_session_id,
    CONCAT(user_pseudo_id, '-', CAST(ga_session_id AS STRING)) AS session_id,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number') AS ga_session_number,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'batch_page_id') AS batch_page_id,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'batch_ordering_id') AS batch_ordering_id,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'batch_event_index') AS batch_event_index,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'session_engaged') AS session_engaged,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'engagement_time_msec') AS engagement_time_msec,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "engaged_session_event") AS engaged_session_event,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "page_location") AS page_location,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "page_title") AS page_title,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "page_referrer") AS page_referrer,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'entrances') AS entrances,
    -- 流入元関連
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "source") AS source,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "medium") AS medium,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "campaign") AS campaign,
    -- ファイルダウンロード・クリック関連
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "file_extension") AS downloaded_file_extension,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "file_name") AS downloaded_file_name,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "link_text") AS downloaded_file_link_text,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "link_url") AS clicked_link_url, -- ファイルダウンロード共用
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "link_domain") AS clicked_link_domain,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "link_classes") AS clicked_link_classes,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "outbound") AS outbound,
    -- 動画再生関連
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "video_provider") AS video_provider,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "video_url") AS video_url,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "video_title") AS video_title,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "visible") AS video_visible,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'video_duration') AS video_duration,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'video_percent') AS video_percent

FROM `your-project.your_dataset.events_*`
WHERE _TABLE_SUFFIX BETWEEN '202401201' AND '20251231'
ORDER BY event_timestamp_utc ASC
